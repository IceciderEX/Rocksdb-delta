//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker_level.h"

#include <algorithm>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "db/delta_l0.h"
#include "db/version_edit.h"
#include "logging/log_buffer.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

bool LevelCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  if (!vstorage->ExpiredTtlFiles().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
    return true;
  }
  if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForForcedBlobGC().empty()) {
    return true;
  }
  for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
    if (vstorage->CompactionScore(i) >= 1) {
      return true;
    }
  }
  return false;
}

namespace {
// Delta L0 compaction 的分区排序 key。
// 用于把 L0 文件按 Delta L0 partition 维度归组和排序，
// 从而让 compaction picker 优先处理更“紧迫”的 partition。
struct DeltaL0CompactionPartitionKey {
  // 表示该文件是否带有有效的 Delta L0 元信息。
  // 新格式文件一般为 true；旧文件或未填充元信息的文件为 false。
  
  bool has_valid_meta = false;
  // Delta L0 partition 的唯一标识。
  // 仅当 has_valid_meta 为 true 时有效。
  uint64_t partition_id = 0;

  // Delta L0 partition 的 generation。
  // partition split 后 generation 会变化，用于区分旧分区和新分区。
  // 仅当 has_valid_meta 为 true 时有效。
  uint64_t partition_generation = 0;

  // 旧格式文件没有 partition_id / generation。
  // 为了避免把所有 legacy 文件错误地归到同一个 partition，
  // 使用文件号作为 fallback key，保证排序稳定且确定。
  uint64_t legacy_file_number = 0;

  // 定义 map key 的排序规则。
  // 排序顺序依次为：
  // 1. 是否有有效 Delta L0 meta；
  // 2. partition_id；
  // 3. partition_generation；
  // 4. legacy_file_number。
  //
  // 这样相同 partition_id / generation 的文件会被归为同一组，
  // 没有有效 meta 的 legacy 文件则按文件号各自独立分组。
  bool operator<(const DeltaL0CompactionPartitionKey& other) const {
    if (has_valid_meta != other.has_valid_meta) {
      return has_valid_meta < other.has_valid_meta;
    }
    if (partition_id != other.partition_id) {
      return partition_id < other.partition_id;
    }
    if (partition_generation != other.partition_generation) {
      return partition_generation < other.partition_generation;
    }
    return legacy_file_number < other.legacy_file_number;
  }
};

// 记录某个 Delta L0 compaction partition 下的文件统计信息。
// 后续会根据总大小和文件数计算 partition 的 compaction 优先级。
struct DeltaL0CompactionPartitionStats {
  // 该 partition 下所有 L0 文件的总字节数。
  uint64_t total_bytes = 0;

  // 该 partition 下的 L0 文件数量。
  uint64_t file_count = 0;
};

// 判断当前 level 是否应该使用 Delta L0 partition 优先级策略。
// 只有 Delta Column Family 的 L0 层才启用该策略。
// 普通 CF 或非 L0 层仍然使用原有 compaction 优先级逻辑。
bool UseDeltaL0PartitionPriority(const std::string& cf_name, int level) {
  return level == 0 && IsDeltaColumnFamilyName(cf_name);
}

// 根据单个 L0 文件的元信息生成 Delta L0 compaction partition key。
// 如果文件带有有效的 delta_l0_meta，则使用 partition_id 和 generation
// 作为分组依据；否则认为它是 legacy 文件，使用文件号作为 fallback，
// 避免多个旧文件被错误合并到同一个虚拟 partition。
DeltaL0CompactionPartitionKey GetDeltaL0CompactionPartitionKey(
    const FileMetaData& file_meta) {
  DeltaL0CompactionPartitionKey key;
  if (file_meta.delta_l0_meta.valid) {
    key.has_valid_meta = true;
    key.partition_id = file_meta.delta_l0_meta.partition_id;
    key.partition_generation = file_meta.delta_l0_meta.partition_generation;
  } else {
    // Keep legacy files deterministic without inventing a shared partition.
    key.legacy_file_number = file_meta.fd.GetNumber();
  }
  return key;
}

// 计算某个 Delta L0 partition 的 compaction 优先级分数。
// 分数由两部分组成：
// 1. 当前 partition 文件总大小 / max_compaction_bytes；
// 2. 当前 partition 文件数 / level0_file_num_compaction_trigger。
//
// 分数越高，说明该 partition 中的数据量或文件数越接近/超过阈值，
// 因此越应该优先被选中进行 L0 内部 compaction。
double ScoreDeltaL0CompactionPartition(
    const DeltaL0CompactionPartitionStats& stats,
    const MutableCFOptions& mutable_cf_options) {
  const double target_l0_bytes = static_cast<double>(
      std::max<uint64_t>(1, mutable_cf_options.max_compaction_bytes));
  const double target_l0_files = static_cast<double>(
      std::max(1, mutable_cf_options.level0_file_num_compaction_trigger));
  return static_cast<double>(stats.total_bytes) / target_l0_bytes +
         static_cast<double>(stats.file_count) / target_l0_files;
}

// 遍历当前 L0 的所有文件，按 Delta L0 compaction partition key 进行归组，
// 并统计每个 partition 的总大小和文件数量。
// 返回结果用于后续计算每个 partition 的 compaction 优先级。
std::map<DeltaL0CompactionPartitionKey, DeltaL0CompactionPartitionStats>
BuildDeltaL0CompactionPartitionStats(
    const std::vector<FileMetaData*>& level_files) {
  std::map<DeltaL0CompactionPartitionKey, DeltaL0CompactionPartitionStats>
      partition_stats;
  for (const FileMetaData* file_meta : level_files) {
    DeltaL0CompactionPartitionStats& stats =
        partition_stats[GetDeltaL0CompactionPartitionKey(*file_meta)];
    stats.total_bytes += file_meta->fd.GetFileSize();
    ++stats.file_count;
  }
  return partition_stats;
}

// 根据 Delta L0 partition 的统计信息，重新构造 L0 文件的 compaction 优先级顺序。
// 输入 file_order 是原始文件顺序；输出 prioritized 是按 partition 压力排序后的文件顺序。
//
// 排序逻辑：
// 1. 先按 partition score 从高到低排序；
// 2. score 相同则文件数更多的 partition 优先；
// 3. 文件数也相同则总字节数更大的 partition 优先；
// 4. 如果仍然相同，保持 stable_sort 的原始相对顺序。
//
// 这样可以让同一个高压力 partition 下的文件更早进入 compaction 候选集，
// 避免 L0 内部 compaction 在多个 Delta partition 之间过度分散。
std::vector<int> BuildDeltaL0CompactionPriorityOrder(
    const std::vector<FileMetaData*>& level_files,
    const std::vector<int>& file_order,
    const MutableCFOptions& mutable_cf_options) {
  std::vector<int> prioritized = file_order;
  if (prioritized.size() <= 1) {
    return prioritized;
  }

  const auto partition_stats = BuildDeltaL0CompactionPartitionStats(level_files);
  std::stable_sort(
      prioritized.begin(), prioritized.end(),
      [&](int lhs_index, int rhs_index) {
        const auto lhs_key =
            GetDeltaL0CompactionPartitionKey(*level_files[lhs_index]);
        const auto rhs_key =
            GetDeltaL0CompactionPartitionKey(*level_files[rhs_index]);
        const auto lhs_it = partition_stats.find(lhs_key);
        const auto rhs_it = partition_stats.find(rhs_key);
        assert(lhs_it != partition_stats.end());
        assert(rhs_it != partition_stats.end());

        const double lhs_score =
            ScoreDeltaL0CompactionPartition(lhs_it->second, mutable_cf_options);
        const double rhs_score =
            ScoreDeltaL0CompactionPartition(rhs_it->second, mutable_cf_options);
        if (lhs_score != rhs_score) {
          return lhs_score > rhs_score;
        }
        if (lhs_it->second.file_count != rhs_it->second.file_count) {
          return lhs_it->second.file_count > rhs_it->second.file_count;
        }
        if (lhs_it->second.total_bytes != rhs_it->second.total_bytes) {
          return lhs_it->second.total_bytes > rhs_it->second.total_bytes;
        }
        return false;
      });
  return prioritized;
}

// 构建 Delta L0 内部 compaction 的候选文件列表。
// 该函数会先生成 L0 文件的自然顺序索引，然后根据 Delta L0 partition
// 的优先级重新排序，最后返回排序后的 FileMetaData* 列表。
//
// 返回结果用于 compaction picker，使其更倾向于优先压缩：
// 1. 文件数较多的 Delta L0 partition；
// 2. 数据量较大的 Delta L0 partition；
// 3. 已经超过配置阈值、compaction 压力更高的 partition。
std::vector<FileMetaData*> BuildDeltaL0IntraL0CompactionCandidates(
    const std::vector<FileMetaData*>& level_files,
    const MutableCFOptions& mutable_cf_options) {
  std::vector<int> natural_order;
  natural_order.reserve(level_files.size());
  for (size_t index = 0; index < level_files.size(); ++index) {
    natural_order.push_back(static_cast<int>(index));
  }
  const std::vector<int> prioritized_order =
      BuildDeltaL0CompactionPriorityOrder(level_files, natural_order,
                                          mutable_cf_options);
  std::vector<FileMetaData*> prioritized_files;
  prioritized_files.reserve(prioritized_order.size());
  for (int index : prioritized_order) {
    prioritized_files.push_back(level_files[index]);
  }
  return prioritized_files;
}

// A class to build a leveled compaction step-by-step.
class LevelCompactionBuilder {
 public:
  LevelCompactionBuilder(const std::string& cf_name,
                         VersionStorageInfo* vstorage,
                         CompactionPicker* compaction_picker,
                         LogBuffer* log_buffer,
                         const MutableCFOptions& mutable_cf_options,
                         const ImmutableOptions& ioptions,
                         const MutableDBOptions& mutable_db_options)
      : cf_name_(cf_name),
        vstorage_(vstorage),
        compaction_picker_(compaction_picker),
        log_buffer_(log_buffer),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions),
        mutable_db_options_(mutable_db_options) {}

  // Pick and return a compaction.
  Compaction* PickCompaction();

  // Pick the initial files to compact to the next level. (or together
  // in Intra-L0 compactions)
  void SetupInitialFiles();

  // If the initial files are from L0 level, pick other L0
  // files if needed.
  bool SetupOtherL0FilesIfNeeded();

  // Compaction with round-robin compaction priority allows more files to be
  // picked to form a large compaction
  void SetupOtherFilesWithRoundRobinExpansion();
  // Based on initial files, setup other files need to be compacted
  // in this compaction, accordingly.
  bool SetupOtherInputsIfNeeded();

  Compaction* GetCompaction();

  // For the specfied level, pick a file that we want to compact.
  // Returns false if there is no file to compact.
  // If it returns true, inputs->files.size() will be exactly one for
  // all compaction priorities except round-robin. For round-robin,
  // multiple consecutive files may be put into inputs->files.
  // If level is 0 and there is already a compaction on that level, this
  // function will return false.
  bool PickFileToCompact();

  // Return true if a L0 trivial move is picked up.
  bool TryPickL0TrivialMove();

  // For L0->L0, picks the longest span of files that aren't currently
  // undergoing compaction for which work-per-deleted-file decreases. The span
  // always starts from the newest L0 file.
  //
  // Intra-L0 compaction is independent of all other files, so it can be
  // performed even when L0->base_level compactions are blocked.
  //
  // Returns true if `inputs` is populated with a span of files to be compacted;
  // otherwise, returns false.
  bool PickIntraL0Compaction();

  // Return true if TrivialMove is extended. `start_index` is the index of
  // the intiial file picked, which should already be in `start_level_inputs_`.
  bool TryExtendNonL0TrivialMove(int start_index);

  // Picks a file from level_files to compact.
  // level_files is a vector of (level, file metadata) in ascending order of
  // level. If compact_to_next_level is true, compact the file to the next
  // level, otherwise, compact to the same level as the input file.
  void PickFileToCompact(
      const autovector<std::pair<int, FileMetaData*>>& level_files,
      bool compact_to_next_level);

  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;
  int start_level_ = -1;
  int output_level_ = -1;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_manual_ = false;
  bool is_l0_trivial_move_ = false;
  CompactionInputFiles start_level_inputs_;
  std::vector<CompactionInputFiles> compaction_inputs_;
  CompactionInputFiles output_level_inputs_;
  std::vector<FileMetaData*> grandparents_;
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableOptions& ioptions_;
  const MutableDBOptions& mutable_db_options_;
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            int level);

  static const int kMinFilesForIntraL0Compaction = 4;
};

void LevelCompactionBuilder::PickFileToCompact(
    const autovector<std::pair<int, FileMetaData*>>& level_files,
    bool compact_to_next_level) {
  for (auto& level_file : level_files) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    start_level_ = level_file.first;
    if ((compact_to_next_level &&
         start_level_ == vstorage_->num_non_empty_levels() - 1) ||
        (start_level_ == 0 &&
         !compaction_picker_->level0_compactions_in_progress()->empty())) {
      continue;
    }
    if (compact_to_next_level) {
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
    } else {
      output_level_ = start_level_;
    }
    start_level_inputs_.files = {level_file.second};
    start_level_inputs_.level = start_level_;
    if (compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                   &start_level_inputs_)) {
      return;
    }
  }
  start_level_inputs_.files.clear();
}

void LevelCompactionBuilder::SetupInitialFiles() {
  // Find the compactions by size on all levels.
  bool skipped_l0_to_base = false;
  for (int i = 0; i < compaction_picker_->NumberLevels() - 1; i++) {
    start_level_score_ = vstorage_->CompactionScore(i);
    start_level_ = vstorage_->CompactionScoreLevel(i);
    assert(i == 0 || start_level_score_ <= vstorage_->CompactionScore(i - 1));
    if (start_level_score_ >= 1) {
      if (skipped_l0_to_base && start_level_ == vstorage_->base_level()) {
        // If L0->base_level compaction is pending, don't schedule further
        // compaction from base level. Otherwise L0->base_level compaction
        // may starve.
        continue;
      }
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
      bool picked_file_to_compact = PickFileToCompact();
      TEST_SYNC_POINT_CALLBACK("PostPickFileToCompact",
                               &picked_file_to_compact);
      if (picked_file_to_compact) {
        // found the compaction!
        if (start_level_ == 0) {
          // L0 score = `num L0 files` / `level0_file_num_compaction_trigger`
          compaction_reason_ = CompactionReason::kLevelL0FilesNum;
        } else {
          // L1+ score = `Level files size` / `MaxBytesForLevel`
          compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
        }
        break;
      } else {
        // didn't find the compaction, clear the inputs
        start_level_inputs_.clear();
        if (start_level_ == 0) {
          skipped_l0_to_base = true;
          // L0->base_level may be blocked due to ongoing L0->base_level
          // compactions. It may also be blocked by an ongoing compaction from
          // base_level downwards.
          //
          // In these cases, to reduce L0 file count and thus reduce likelihood
          // of write stalls, we can attempt compacting a span of files within
          // L0.
          if (PickIntraL0Compaction()) {
            output_level_ = 0;
            compaction_reason_ = CompactionReason::kLevelL0FilesNum;
            break;
          }
        }
      }
    } else {
      // Compaction scores are sorted in descending order, no further scores
      // will be >= 1.
      break;
    }
  }
  if (!start_level_inputs_.empty()) {
    return;
  }

  // if we didn't find a compaction, check if there are any files marked for
  // compaction
  parent_index_ = base_index_ = -1;

  compaction_picker_->PickFilesMarkedForCompaction(
      cf_name_, vstorage_, &start_level_, &output_level_, &start_level_inputs_);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kFilesMarkedForCompaction;
    return;
  }

  // Bottommost Files Compaction on deleting tombstones
  PickFileToCompact(vstorage_->BottommostFilesMarkedForCompaction(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kBottommostFiles;
    return;
  }

  // TTL Compaction
  if (ioptions_.compaction_pri == kRoundRobin &&
      !vstorage_->ExpiredTtlFiles().empty()) {
    auto expired_files = vstorage_->ExpiredTtlFiles();
    // the expired files list should already be sorted by level
    start_level_ = expired_files.front().first;
#ifndef NDEBUG
    for (const auto& file : expired_files) {
      assert(start_level_ <= file.first);
    }
#endif
    if (start_level_ > 0) {
      output_level_ = start_level_ + 1;
      if (PickFileToCompact()) {
        compaction_reason_ = CompactionReason::kRoundRobinTtl;
        return;
      }
    }
  }

  PickFileToCompact(vstorage_->ExpiredTtlFiles(), true);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kTtl;
    return;
  }

  // Periodic Compaction
  PickFileToCompact(vstorage_->FilesMarkedForPeriodicCompaction(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kPeriodicCompaction;
    return;
  }

  // Forced blob garbage collection
  PickFileToCompact(vstorage_->FilesMarkedForForcedBlobGC(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kForcedBlobGC;
    return;
  }
}

bool LevelCompactionBuilder::SetupOtherL0FilesIfNeeded() {
  if (start_level_ == 0 && output_level_ != 0 && !is_l0_trivial_move_) {
    return compaction_picker_->GetOverlappingL0Files(
        vstorage_, &start_level_inputs_, output_level_, &parent_index_);
  }
  return true;
}

void LevelCompactionBuilder::SetupOtherFilesWithRoundRobinExpansion() {
  // We only expand when the start level is not L0 under round robin
  assert(start_level_ >= 1);

  // For round-robin compaction priority, we have 3 constraints when picking
  // multiple files.
  // Constraint 1: We can only pick consecutive files
  //  -> Constraint 1a: When a file is being compacted (or some input files
  //                    are being compacted after expanding, we cannot
  //                    choose it and have to stop choosing more files
  //  -> Constraint 1b: When we reach the last file (with largest keys), we
  //                    cannot choose more files (the next file will be the
  //                    first one)
  // Constraint 2: We should ensure the total compaction bytes (including the
  //               overlapped files from the next level) is no more than
  //               mutable_cf_options_.max_compaction_bytes
  // Constraint 3: We try our best to pick as many files as possible so that
  //               the post-compaction level size is less than
  //               MaxBytesForLevel(start_level_)
  // Constraint 4: We do not expand if it is possible to apply a trivial move
  // Constraint 5 (TODO): Try to pick minimal files to split into the target
  //               number of subcompactions
  TEST_SYNC_POINT("LevelCompactionPicker::RoundRobin");

  // Only expand the inputs when we have selected a file in start_level_inputs_
  if (start_level_inputs_.size() == 0) return;

  uint64_t start_lvl_bytes_no_compacting = 0;
  uint64_t curr_bytes_to_compact = 0;
  uint64_t start_lvl_max_bytes_to_compact = 0;
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);
  // Constraint 3 (pre-calculate the ideal max bytes to compact)
  for (auto f : level_files) {
    if (!f->being_compacted) {
      start_lvl_bytes_no_compacting += f->fd.GetFileSize();
    }
  }
  if (start_lvl_bytes_no_compacting >
      vstorage_->MaxBytesForLevel(start_level_)) {
    start_lvl_max_bytes_to_compact = start_lvl_bytes_no_compacting -
                                     vstorage_->MaxBytesForLevel(start_level_);
  }

  size_t start_index = vstorage_->FilesByCompactionPri(start_level_)[0];
  InternalKey smallest, largest;
  // Constraint 4 (No need to check again later)
  compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
  CompactionInputFiles output_level_inputs;
  output_level_inputs.level = output_level_;
  vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                  &output_level_inputs.files);
  if (output_level_inputs.empty()) {
    if (TryExtendNonL0TrivialMove((int)start_index)) {
      return;
    }
  }
  // Constraint 3
  if (start_level_inputs_[0]->fd.GetFileSize() >=
      start_lvl_max_bytes_to_compact) {
    return;
  }
  CompactionInputFiles tmp_start_level_inputs;
  tmp_start_level_inputs = start_level_inputs_;
  // TODO (zichen): Future parallel round-robin may also need to update this
  // Constraint 1b (only expand till the end)
  for (size_t i = start_index + 1; i < level_files.size(); i++) {
    auto* f = level_files[i];
    if (f->being_compacted) {
      // Constraint 1a
      return;
    }

    tmp_start_level_inputs.files.push_back(f);
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &tmp_start_level_inputs) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {tmp_start_level_inputs}, output_level_,
            Compaction::EvaluatePenultimateLevel(
                vstorage_, ioptions_, start_level_, output_level_))) {
      // Constraint 1a
      tmp_start_level_inputs.clear();
      return;
    }

    curr_bytes_to_compact = 0;
    for (auto start_lvl_f : tmp_start_level_inputs.files) {
      curr_bytes_to_compact += start_lvl_f->fd.GetFileSize();
    }

    // Check whether any output level files are locked
    compaction_picker_->GetRange(tmp_start_level_inputs, &smallest, &largest);
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (!output_level_inputs.empty() &&
        !compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &output_level_inputs)) {
      // Constraint 1a
      tmp_start_level_inputs.clear();
      return;
    }

    uint64_t start_lvl_curr_bytes_to_compact = curr_bytes_to_compact;
    for (auto output_lvl_f : output_level_inputs.files) {
      curr_bytes_to_compact += output_lvl_f->fd.GetFileSize();
    }
    if (curr_bytes_to_compact > mutable_cf_options_.max_compaction_bytes) {
      // Constraint 2
      tmp_start_level_inputs.clear();
      return;
    }

    start_level_inputs_.files = tmp_start_level_inputs.files;
    // Constraint 3
    if (start_lvl_curr_bytes_to_compact > start_lvl_max_bytes_to_compact) {
      return;
    }
  }
}

bool LevelCompactionBuilder::SetupOtherInputsIfNeeded() {
  // Setup input files from output level. For output to L0, we only compact
  // spans of files that do not interact with any pending compactions, so don't
  // need to consider other levels.
  if (output_level_ != 0) {
    output_level_inputs_.level = output_level_;
    bool round_robin_expanding =
        ioptions_.compaction_pri == kRoundRobin &&
        compaction_reason_ == CompactionReason::kLevelMaxLevelSize;
    if (round_robin_expanding) {
      SetupOtherFilesWithRoundRobinExpansion();
    }
    if (!is_l0_trivial_move_ &&
        !compaction_picker_->SetupOtherInputs(
            cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs_,
            &output_level_inputs_, &parent_index_, base_index_,
            round_robin_expanding)) {
      return false;
    }

    compaction_inputs_.push_back(start_level_inputs_);
    if (!output_level_inputs_.empty()) {
      compaction_inputs_.push_back(output_level_inputs_);
    }

    // In some edge cases we could pick a compaction that will be compacting
    // a key range that overlap with another running compaction, and both
    // of them have the same output level. This could happen if
    // (1) we are running a non-exclusive manual compaction
    // (2) AddFile ingest a new file into the LSM tree
    // We need to disallow this from happening.
    if (compaction_picker_->FilesRangeOverlapWithCompaction(
            compaction_inputs_, output_level_,
            Compaction::EvaluatePenultimateLevel(
                vstorage_, ioptions_, start_level_, output_level_))) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      return false;
    }
    if (!is_l0_trivial_move_) {
      compaction_picker_->GetGrandparents(vstorage_, start_level_inputs_,
                                          output_level_inputs_, &grandparents_);
    }
  } else {
    compaction_inputs_.push_back(start_level_inputs_);
  }
  return true;
}

Compaction* LevelCompactionBuilder::PickCompaction() {
  // Pick up the first file to start compaction. It may have been extended
  // to a clean cut.
  SetupInitialFiles();
  if (start_level_inputs_.empty()) {
    return nullptr;
  }
  assert(start_level_ >= 0 && output_level_ >= 0);

  // If it is a L0 -> base level compaction, we need to set up other L0
  // files if needed.
  if (!SetupOtherL0FilesIfNeeded()) {
    return nullptr;
  }

  // Pick files in the output level and expand more files in the start level
  // if needed.
  if (!SetupOtherInputsIfNeeded()) {
    return nullptr;
  }

  // Form a compaction object containing the files we picked.
  Compaction* c = GetCompaction();

  TEST_SYNC_POINT_CALLBACK("LevelCompactionPicker::PickCompaction:Return", c);

  return c;
}

Compaction* LevelCompactionBuilder::GetCompaction() {
  auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(compaction_inputs_), output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          ioptions_.compaction_style, vstorage_->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options_.max_compaction_bytes,
      GetPathId(ioptions_, mutable_cf_options_, output_level_),
      GetCompressionType(vstorage_, mutable_cf_options_, output_level_,
                         vstorage_->base_level()),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level_),
      Temperature::kUnknown,
      /* max_subcompactions */ 0, std::move(grandparents_), is_manual_,
      /* trim_ts */ "", start_level_score_, false /* deletion_compaction */,
      /* l0_files_might_overlap */ start_level_ == 0 && !is_l0_trivial_move_,
      compaction_reason_);

  // If it's level 0 compaction, make sure we don't execute any other level 0
  // compactions in parallel
  compaction_picker_->RegisterCompaction(c);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  return c;
}

/*
 * Find the optimal path to place a file
 * Given a level, finds the path where levels up to it will fit in levels
 * up to and including this path
 */
uint32_t LevelCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, int level) {
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = ioptions.cf_paths[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = mutable_cf_options.max_bytes_for_level_base;

  // Last path is the fallback
  while (p < ioptions.cf_paths.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > 0) {
          if (ioptions.level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/main/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier);
          } else {
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier *
                mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = ioptions.cf_paths[p].target_size;
  }
  return p;
}

bool LevelCompactionBuilder::TryPickL0TrivialMove() {
  if (vstorage_->base_level() <= 0) {
    return false;
  }
  if (start_level_ == 0 && mutable_cf_options_.compression_per_level.empty() &&
      !vstorage_->LevelFiles(output_level_).empty() &&
      ioptions_.db_paths.size() <= 1) {
    // Try to pick trivial move from L0 to L1. We start from the oldest
    // file. We keep expanding to newer files if it would form a
    // trivial move.
    // For now we don't support it with
    // mutable_cf_options_.compression_per_level to prevent the logic
    // of determining whether L0 can be trivial moved to the next level.
    // We skip the case where output level is empty, since in this case, at
    // least the oldest file would qualify for trivial move, and this would
    // be a surprising behavior with few benefits.

    // We search from the oldest file from the newest. In theory, there are
    // files in the middle can form trivial move too, but it is probably
    // uncommon and we ignore these cases for simplicity.
    const std::vector<FileMetaData*>& level_files =
        vstorage_->LevelFiles(start_level_);

    InternalKey my_smallest, my_largest;
    for (auto it = level_files.rbegin(); it != level_files.rend(); ++it) {
      CompactionInputFiles output_level_inputs;
      output_level_inputs.level = output_level_;
      FileMetaData* file = *it;
      if (it == level_files.rbegin()) {
        my_smallest = file->smallest;
        my_largest = file->largest;
      } else {
        if (compaction_picker_->icmp()->Compare(file->largest, my_smallest) <
            0) {
          my_smallest = file->smallest;
        } else if (compaction_picker_->icmp()->Compare(file->smallest,
                                                       my_largest) > 0) {
          my_largest = file->largest;
        } else {
          break;
        }
      }
      vstorage_->GetOverlappingInputs(output_level_, &my_smallest, &my_largest,
                                      &output_level_inputs.files);
      if (output_level_inputs.empty()) {
        assert(!file->being_compacted);
        start_level_inputs_.files.push_back(file);
      } else {
        break;
      }
    }
  }

  if (!start_level_inputs_.empty()) {
    // Sort files by key range. Not sure it's 100% necessary but it's cleaner
    // to always keep files sorted by key the key ranges don't overlap.
    std::sort(start_level_inputs_.files.begin(),
              start_level_inputs_.files.end(),
              [icmp = compaction_picker_->icmp()](FileMetaData* f1,
                                                  FileMetaData* f2) -> bool {
                return (icmp->Compare(f1->smallest, f2->smallest) < 0);
              });

    is_l0_trivial_move_ = true;
    return true;
  }
  return false;
}

bool LevelCompactionBuilder::TryExtendNonL0TrivialMove(int start_index) {
  if (start_level_inputs_.size() == 1 &&
      (ioptions_.db_paths.empty() || ioptions_.db_paths.size() == 1) &&
      (mutable_cf_options_.compression_per_level.empty())) {
    // Only file of `index`, and it is likely a trivial move. Try to
    // expand if it is still a trivial move, but not beyond
    // max_compaction_bytes or 4 files, so that we don't create too
    // much compaction pressure for the next level.
    // Ignore if there are more than one DB path, as it would be hard
    // to predict whether it is a trivial move.
    const std::vector<FileMetaData*>& level_files =
        vstorage_->LevelFiles(start_level_);
    const size_t kMaxMultiTrivialMove = 4;
    FileMetaData* initial_file = start_level_inputs_.files[0];
    size_t total_size = initial_file->fd.GetFileSize();
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    for (int i = start_index + 1;
         i < static_cast<int>(level_files.size()) &&
         start_level_inputs_.size() < kMaxMultiTrivialMove;
         i++) {
      FileMetaData* next_file = level_files[i];
      if (next_file->being_compacted) {
        break;
      }
      vstorage_->GetOverlappingInputs(output_level_, &(initial_file->smallest),
                                      &(next_file->largest),
                                      &output_level_inputs.files);
      if (!output_level_inputs.empty()) {
        break;
      }
      if (i < static_cast<int>(level_files.size()) - 1 &&
          compaction_picker_->icmp()
                  ->user_comparator()
                  ->CompareWithoutTimestamp(
                      next_file->largest.user_key(),
                      level_files[i + 1]->smallest.user_key()) == 0) {
        TEST_SYNC_POINT_CALLBACK(
            "LevelCompactionBuilder::TryExtendNonL0TrivialMove:NoCleanCut",
            nullptr);
        // Not a clean up after adding the next file. Skip.
        break;
      }
      total_size += next_file->fd.GetFileSize();
      if (total_size > mutable_cf_options_.max_compaction_bytes) {
        break;
      }
      start_level_inputs_.files.push_back(next_file);
    }
    return start_level_inputs_.size() > 1;
  }
  return false;
}

bool LevelCompactionBuilder::PickFileToCompact() {
  // level 0 files are overlapping. So we cannot pick more
  // than one concurrent compactions at this level. This
  // could be made better by looking at key-ranges that are
  // being compacted at level 0.
  if (start_level_ == 0 &&
      !compaction_picker_->level0_compactions_in_progress()->empty()) {
    TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
    return false;
  }

  start_level_inputs_.clear();
  start_level_inputs_.level = start_level_;

  assert(start_level_ >= 0);

  if (TryPickL0TrivialMove()) {
    return true;
  }

  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);

  // Pick the file with the highest score in this level that is not already
  // being compacted.
  const std::vector<int>& file_scores =
      vstorage_->FilesByCompactionPri(start_level_);
  // Delta L0 partition priority 使用单独的文件顺序数组。
  // 默认情况下 candidate_order 指向 RocksDB 原有的 file_scores；
  // 如果当前是 Delta Column Family 的 L0 层，则会重新构造一个
  // 按 Delta L0 partition 压力排序后的 delta_file_scores。
  std::vector<int> delta_file_scores;
  const std::vector<int>* candidate_order = &file_scores;
  // 判断当前 level 是否启用 Delta L0 partition-aware 的 compaction 优先级。
  // 只有 Delta Column Family 的 L0 层会启用；普通 CF 或非 L0 层保持原逻辑。
  const bool use_delta_l0_partition_priority =
      UseDeltaL0PartitionPriority(cf_name_, start_level_);
  if (use_delta_l0_partition_priority) {
    // 对 Delta L0 文件按 partition 维度重新排序。
    // 排序依据来自每个 partition 的文件数量和总大小：
    // 压力更高的 partition 会排在更前面，从而更早被 compaction picker 选中。
    //
    // 注意这里没有改变 level_files 本身，只是生成一个新的索引顺序。
    delta_file_scores = BuildDeltaL0CompactionPriorityOrder(
        level_files, file_scores, mutable_cf_options_);

    // 后续遍历候选文件时，改用 Delta partition-aware 的候选顺序。
    candidate_order = &delta_file_scores;
  }

  unsigned int cmp_idx;
  // 普通 compaction priority 使用 vstorage_ 中记录的 NextCompactionIndex，
  // 以支持原有 round-robin / cursor 行为。
  //
  // Delta L0 partition priority 每次都从重新排序后的候选列表开头扫描，
  // 因为该列表已经根据当前 partition 压力动态排序；
  // 如果沿用旧的 NextCompactionIndex，可能会跳过当前最需要 compact 的 partition。
  const unsigned int initial_cmp_idx =
      use_delta_l0_partition_priority
          ? 0
          : static_cast<unsigned int>(vstorage_->NextCompactionIndex(
                start_level_));
  for (cmp_idx = initial_cmp_idx; cmp_idx < candidate_order->size();
       cmp_idx++) {
    // candidate_order 可能是原始 file_scores，也可能是 Delta L0 partition
    // 重新排序后的 delta_file_scores。这里统一通过索引取出候选文件。
    int index = (*candidate_order)[cmp_idx];
    auto* f = level_files[index];

    // do not pick a file to compact if it is being compacted
    // from n-1 level.
    if (f->being_compacted) {
      if (ioptions_.compaction_pri == kRoundRobin) {
        // TODO(zichen): this file may be involved in one compaction from
        // an upper level, cannot advance the cursor for round-robin policy.
        // Currently, we do not pick any file to compact in this case. We
        // should fix this later to ensure a compaction is picked but the
        // cursor shall not be advanced.
        return false;
      }
      continue;
    }

    start_level_inputs_.files.push_back(f);
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &start_level_inputs_) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {start_level_inputs_}, output_level_,
            Compaction::EvaluatePenultimateLevel(
                vstorage_, ioptions_, start_level_, output_level_))) {
      // A locked (pending compaction) input-level file was pulled in due to
      // user-key overlap.
      start_level_inputs_.clear();

      if (ioptions_.compaction_pri == kRoundRobin) {
        return false;
      }
      continue;
    }

    // Now that input level is fully expanded, we check whether any output
    // files are locked due to pending compaction.
    //
    // Note we rely on ExpandInputsToCleanCut() to tell us whether any output-
    // level files are locked, not just the extra ones pulled in for user-key
    // overlap.
    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (output_level_inputs.empty()) {
      if (TryExtendNonL0TrivialMove(index)) {
        break;
      }
    } else {
      if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                      &output_level_inputs)) {
        start_level_inputs_.clear();
        if (ioptions_.compaction_pri == kRoundRobin) {
          return false;
        }
        continue;
      }
    }

    base_index_ = index;
    break;
  }

  // store where to start the iteration in the next call to PickCompaction
  if (ioptions_.compaction_pri != kRoundRobin) {
    // 普通路径继续记录下一次 compaction 的扫描起点。
    //
    // Delta L0 partition priority 路径固定写回 0：
    // 因为下一次调用会重新基于最新 L0 文件分布计算 partition score，
    // 应该重新从最高优先级 partition 开始，而不是沿用本次扫描到的位置。
    vstorage_->SetNextCompactionIndex(
        start_level_, use_delta_l0_partition_priority ? 0
                                                      : static_cast<int>(cmp_idx));
  }
  return start_level_inputs_.size() > 0;
}

bool LevelCompactionBuilder::PickIntraL0Compaction() {
  start_level_inputs_.clear();
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(0 /* level */);
  if (level_files.size() <
          static_cast<size_t>(
              mutable_cf_options_.level0_file_num_compaction_trigger + 2) ||
      level_files[0]->being_compacted) {
    // If L0 isn't accumulating much files beyond the regular trigger, don't
    // resort to L0->L0 compaction yet.
    return false;
  }
  // Delta Column Family 的 L0->L0 compaction 优先使用 partition-aware
  // 的候选文件顺序。
  //
  // 这样可以让文件数更多、总大小更大、compaction 压力更高的 Delta L0
  // partition 被优先整理，减少 L0 文件在多个 partition 之间无序堆积。
  if (UseDeltaL0PartitionPriority(cf_name_, 0 /* level */)) {
    const std::vector<FileMetaData*> prioritized_level_files =
        BuildDeltaL0IntraL0CompactionCandidates(level_files,
                                                mutable_cf_options_);
    // 构造按 Delta L0 partition 压力排序后的 L0 文件列表。
    // 这里返回的是 FileMetaData* 顺序，不改变 VersionStorage 中原始 L0 文件顺序。
    if (FindIntraL0Compaction(prioritized_level_files,
                              kMinFilesForIntraL0Compaction,
                              std::numeric_limits<uint64_t>::max(),
                              mutable_cf_options_.max_compaction_bytes,
                              &start_level_inputs_)) {
      return true;
    }
    // 如果 Delta partition-aware 顺序没有找到合适的 L0->L0 compaction，
    // 清空临时输入，继续回退到原始 level_files 顺序。
    //
    // 这样可以保证新增的 Delta 优先级策略只是增强候选排序，
    // 不会破坏原有 Intra-L0 compaction 的兜底行为。
    start_level_inputs_.clear();
  }
  return FindIntraL0Compaction(level_files, kMinFilesForIntraL0Compaction,
                               std::numeric_limits<uint64_t>::max(),
                               mutable_cf_options_.max_compaction_bytes,
                               &start_level_inputs_);
}
}  // namespace

Compaction* LevelCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  LevelCompactionBuilder builder(cf_name, vstorage, this, log_buffer,
                                 mutable_cf_options, ioptions_,
                                 mutable_db_options);
  return builder.PickCompaction();
}
}  // namespace ROCKSDB_NAMESPACE
