//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "delta/hotspot_manager.h"

#include <chrono>
#include <sstream>

#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/sst_file_writer.h"

namespace ROCKSDB_NAMESPACE {

HotspotManager::HotspotManager(const Options& db_options,
                               const std::string& data_dir)
    : db_options_(db_options),
      data_dir_(data_dir),
      lifecycle_manager_(std::make_shared<HotSstLifecycleManager>(db_options)),
      index_table_(lifecycle_manager_),
      frequency_table_(4, 600) {
  db_options_.env->CreateDirIfMissing(data_dir_);
}

uint64_t HotspotManager::ExtractCUID(const Slice& key) {
  // TODO: 根据实际的 Key Schema提取 cuid，这里先假设一波
  if (key.size() < 24) {
    return 0;
  }

  const unsigned char* p =
      reinterpret_cast<const unsigned char*>(key.data()) + 16;

  // Big-Endian Decoding
  uint64_t cuid = (static_cast<uint64_t>(p[0]) << 56) |
                  (static_cast<uint64_t>(p[1]) << 48) |
                  (static_cast<uint64_t>(p[2]) << 40) |
                  (static_cast<uint64_t>(p[3]) << 32) |
                  (static_cast<uint64_t>(p[4]) << 24) |
                  (static_cast<uint64_t>(p[5]) << 16) |
                  (static_cast<uint64_t>(p[6]) << 8) |
                  (static_cast<uint64_t>(p[7]));

  return cuid;
}

bool HotspotManager::RegisterScan(uint64_t cuid, bool is_full_scan,
                                  bool* became_hot) {
  if (cuid == 0) {
    if (became_hot) *became_hot = false;
    return false;
  }

  bool first_time_hot = false;
  bool is_hot = frequency_table_.RecordAndCheckHot(cuid, &first_time_hot);

  // 返回是否首次成为热点
  if (became_hot) {
    *became_hot = first_time_hot;
  }

  if (is_hot && !first_time_hot) {
    // 已经是热点（非首次），初始化 pending 列表
    if (ShouldTriggerScanAsCompaction(cuid)) {
      std::lock_guard<std::mutex> lock(pending_mutex_);
      if (pending_snapshots_.find(cuid) == pending_snapshots_.end()) {
        pending_snapshots_[cuid] = std::vector<DataSegment>();
      }
    }
  }

  if (is_full_scan) {
    delete_table_.ResetTracking(cuid);
  }
  return is_hot;
}

bool HotspotManager::BufferHotData(uint64_t cuid, const Slice& key,
                                   const Slice& value) {
  {
    std::lock_guard<std::mutex> lock(buffered_cuids_mutex_);
    active_buffered_cuids_.insert(cuid);
  }
  return buffer_.Append(cuid, key, value);
}

bool HotspotManager::InterceptDelete(const Slice& key) {
  uint64_t cuid = ExtractCUID(key);
  if (cuid == 0) return false;

  // 在 GDCT 中查询是否该 cuid 被标记为删除
  bool marked = delete_table_.MarkDeleted(cuid);

  if (marked) {
    // fprintf(stderr, "[HotspotManager] Intercepted Delete for CUID: %lu\n",
    // cuid);
    return true;
  }

  // CUID 不在热点管理范围内
  return false;
}

std::string HotspotManager::GenerateSstFileName(uint64_t cuid) {
  auto now = std::chrono::system_clock::now();
  auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                       now.time_since_epoch())
                       .count();

  std::stringstream ss;
  ss << data_dir_ << "/hot_" << cuid << "_" << timestamp << ".sst";
  return ss.str();
}

bool HotspotManager::ShouldTriggerScanAsCompaction(uint64_t cuid) {
  HotIndexEntry entry;
  if (!index_table_.GetEntry(cuid, &entry)) {
    // a)	当前热点CUid无Snapshot。
    return true;
  }
  // b)	已有Snapshot，且新增的Deltas片段数量超过5个。
  if (!entry.HasSnapshot() || entry.deltas.size() > 5) {
    return true;
  }
  return false;
}

class VectorIterator {
 public:
  VectorIterator(const std::vector<HotEntry>& data) : data_(data), idx_(0) {}

  bool Valid() const { return idx_ < data_.size(); }
  void Next() { idx_++; }
  const Slice Key() const { return data_[idx_].key; }
  const Slice Value() const { return data_[idx_].value; }
  uint64_t Cuid() const { return data_[idx_].cuid; }

 private:
  const std::vector<HotEntry>& data_;
  size_t idx_;
};

Status HotspotManager::FlushBlockToSharedSST(
    std::unique_ptr<HotDataBlock> block,
    std::unordered_map<uint64_t, DataSegment>* output_segments) {
  if (!block || block->entries.empty()) return Status::OK();

  // keysort
  block->Sort();

  EnvOptions env_options;
  SstFileWriter sst_writer(env_options, db_options_);

  // 文件名使用时间戳
  auto now = std::chrono::system_clock::now();
  uint64_t file_number = std::chrono::duration_cast<std::chrono::microseconds>(
                             now.time_since_epoch())
                             .count();
  std::string file_path =
      data_dir_ + "/hot_shared_" + std::to_string(file_number) + ".sst";

  Status s = sst_writer.Open(file_path);
  if (!s.ok()) return s;

  lifecycle_manager_->RegisterFile(file_number, file_path);

  // 3. 遍历写入并记录 Segment
  auto& entries = block->entries;
  size_t i = 0;
  while (i < entries.size()) {
    uint64_t current_cuid = entries[i].cuid;
    std::string segment_first_key = entries[i].key;  // 记录 First Key
    std::string segment_last_key;

    std::string last_written_key_in_segment;
    bool is_first_entry_in_segment = true;

    uint64_t logical_size = 0;
    int written_count = 0;

    // 当前 CUID 的所有 Entry
    while (i < entries.size() && entries[i].cuid == current_cuid) {
      const auto& current_key = entries[i].key;
      // 去重(在delta表中按理说不存在，因为是append-only key)
      if (!is_first_entry_in_segment &&
          current_key == last_written_key_in_segment) {
        i++;
        continue;
      }
      s = sst_writer.Put(entries[i].key, entries[i].value);
      if (!s.ok()) return s;

      last_written_key_in_segment = current_key;
      segment_last_key = current_key;  // 实时更新 Segment Last Key
      written_count++;
      is_first_entry_in_segment = false;
      i++;
    }

    if (written_count > 0) {
      DataSegment segment;
      segment.file_number = file_number;
      segment.first_key = segment_first_key;
      segment.last_key = segment_last_key;

      (*output_segments)[current_cuid] = segment;
    }
  }

  // 4. Finish
  ExternalSstFileInfo file_info;
  s = sst_writer.Finish(&file_info);
  if (!s.ok()) {
    return s;
  }

  fprintf(stdout, "[HotspotManager] Flushed Shared SST: %s, CUIDs: %lu\n",
          file_path.c_str(), output_segments->size());

  return Status::OK();
}

void HotspotManager::TriggerBufferFlush() {
  // 轮转 Buffer
  if (!buffer_.RotateBuffer()) {
    return;
  }

  // 提取待刷盘 Block
  auto block = buffer_.ExtractBlockToFlush();
  while (block) {
    std::unordered_map<uint64_t, DataSegment> new_segments;
    // to share SST
    Status s = FlushBlockToSharedSST(std::move(block), &new_segments);

    if (s.ok()) {
      for (const auto& kv : new_segments) {
        uint64_t cuid = kv.first;
        const DataSegment& real_segment = kv.second;
        // 直接作为 Snapshot 片段追加?
        // index_table_.AppendSnapshotSegment(kv.first, kv.second);
        auto it = pending_snapshots_.find(cuid);
        if (it != pending_snapshots_.end()) {
          // Case A: Scan 过程中触发的 Flush, Finalize 时一起提交
          it->second.push_back(real_segment);
        } else {
          // Case B: scan 之后被其他cuid数据填满 flush，
          // 检查 Index 中 {-1} 的记录
          bool promoted = index_table_.PromoteSnapshot(cuid, real_segment);
          if (!promoted) {
            // 如果没有 -1?
            index_table_.AppendSnapshotSegment(kv.first, kv.second);
          }
        }
      }
    } else {
      fprintf(stderr, "[HotspotManager] Failed to flush shared block: %s\n",
              s.ToString().c_str());
    }
    block = buffer_.ExtractBlockToFlush();
  }
}

void HotspotManager::FinalizeScanAsCompaction(uint64_t cuid) {
  if (cuid == 0) return;

  std::vector<DataSegment> final_segments;

  // Scan 过程中产生的 Pending SSTs
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_snapshots_.find(cuid);
    if (it != pending_snapshots_.end()) {
      final_segments = it->second;
      pending_snapshots_.erase(it);
    }
  }

  bool has_buffered_data = false;
  {
    std::lock_guard<std::mutex> lock(buffered_cuids_mutex_);
    auto it = active_buffered_cuids_.find(cuid);
    if (it != active_buffered_cuids_.end()) {
      has_buffered_data = true;
      active_buffered_cuids_.erase(it);  // 清理状态
    }
  }

  // 防止空scan写入snapshot的情况
  if (final_segments.empty() && !has_buffered_data) {
    return;
  }

  // tail segment, PromoteSnapshot()填充具体值
  std::string min_key, max_key;
  if (buffer_.GetBoundaryKeys(cuid, &min_key, &max_key)) {
    DataSegment tail_segment;
    tail_segment.file_number = static_cast<uint64_t>(-1);  // 标记为内存段
    tail_segment.first_key = min_key;
    tail_segment.last_key = max_key;

    final_segments.push_back(tail_segment);
  } else {
    fprintf(stderr,
            "[HotspotManager] GetBoundaryKeys failed for CUID %lu. Has "
            "buffered data: %d\n",
            cuid, has_buffered_data ? 1 : 0);
  }

  if (final_segments.empty()) return;

  // 更新这个 cuid 的 Snapshot
  index_table_.UpdateSnapshot(cuid, final_segments);
  index_table_.MarkDeltasAsObsolete(cuid);

  // fprintf(stdout, "[HotspotManager] Finalized CUID %lu. Snapshot has %zu
  // segments (incl tail).\n",
  //         cuid, final_segments.size());
  // DebugDump("AFTER_SCAN_FINALIZE_CUID_" + std::to_string(cuid));
}

ScanAsCompactionStrategy HotspotManager::EvaluateScanAsCompactionStrategy(
    uint64_t cuid, bool is_full_scan, const std::string& scan_first_key,
    const std::string& scan_last_key, size_t* out_involved_delta_count) {
  // 全版本 Scan 使用 FullReplace 策略
  if (is_full_scan) {
    if (out_involved_delta_count) {
      *out_involved_delta_count = 0;
    }
    return ScanAsCompactionStrategy::kFullReplace;
  }

  // 小 Scan: 查看涉及多少个 delta segments
  size_t delta_count = CountInvolvedDeltas(cuid, scan_first_key, scan_last_key);
  if (out_involved_delta_count) {
    *out_involved_delta_count = delta_count;
  }

  // 根据阈值决定策略
  if (delta_count >= kPartialMergeDeltaThreshold) {
    return ScanAsCompactionStrategy::kPartialMerge;
  }

  return ScanAsCompactionStrategy::kNoAction;
}

size_t HotspotManager::CountInvolvedDeltas(uint64_t cuid,
                                           const std::string& first_key,
                                           const std::string& last_key) {
  return index_table_.CountOverlappingDeltas(cuid, first_key, last_key);
}

void HotspotManager::FinalizeScanAsCompactionWithStrategy(
    uint64_t cuid, ScanAsCompactionStrategy strategy,
    const std::string& scan_first_key, const std::string& scan_last_key) {
  if (cuid == 0) return;

  switch (strategy) {
    case ScanAsCompactionStrategy::kNoAction:
      return;
    case ScanAsCompactionStrategy::kFullReplace:
      FinalizeScanAsCompaction(cuid);
      break;
    case ScanAsCompactionStrategy::kPartialMerge:
      EnqueuePartialMerge(cuid, scan_first_key, scan_last_key);
      break;
  }
}

// ----------------- L0Compaction --------------------

void HotspotManager::UpdateCompactionDelta(
    uint64_t cuid, const std::vector<uint64_t>& input_files,
    uint64_t output_file_number, const std::string& first_key,
    const std::string& last_key) {
  if (first_key.empty()) return;

  DataSegment seg;
  seg.file_number = output_file_number;
  seg.first_key = first_key;
  seg.last_key = last_key;

  index_table_.UpdateDeltaIndex(cuid, input_files, seg);
}

bool HotspotManager::ShouldSkipObsoleteDelta(
    uint64_t cuid, const std::vector<uint64_t>& input_files) {
  return index_table_.IsDeltaObsolete(cuid, input_files);
}

void HotspotManager::CleanUpMetadataAfterCompaction(
    const std::unordered_set<uint64_t>& involved_cuids,
    const std::vector<uint64_t>& input_files) {
  if (input_files.empty() || involved_cuids.empty()) return;

  // 步骤d：热点索引表的处理
  index_table_.RemoveObsoleteDeltasForCUIDs(involved_cuids, input_files);

  // 步骤c：已删除cuid hotdata文件的处理
  for (uint64_t cuid : involved_cuids) {
    delete_table_.UntrackFiles(cuid, input_files);
  }
}

bool HotspotManager::IsHot(uint64_t cuid) {
  return frequency_table_.IsHot(cuid);
}

// --------------------- HotspotManager Iterator --------------------- //
InternalIterator* HotspotManager::NewBufferIterator(
    uint64_t cuid, const InternalKeyComparator* icmp) {
  return buffer_.NewIterator(cuid, icmp);
}

// --------------------- Pending Init CUID Queue --------------------- //
void HotspotManager::EnqueueForInitScan(uint64_t cuid) {
  std::lock_guard<std::mutex> lock(pending_init_mutex_);
  // 避免重复添加
  for (uint64_t c : pending_init_cuids_) {
    if (c == cuid) return;
  }
  pending_init_cuids_.push_back(cuid);
  fprintf(stdout, "[HotspotManager] Enqueued CUID %lu for initial full scan\n",
          cuid);
}

std::vector<uint64_t> HotspotManager::PopPendingInitCuids() {
  std::lock_guard<std::mutex> lock(pending_init_mutex_);
  std::vector<uint64_t> result = std::move(pending_init_cuids_);
  pending_init_cuids_.clear();
  return result;
}

bool HotspotManager::HasPendingInitCuids() const {
  std::lock_guard<std::mutex> lock(pending_init_mutex_);
  return !pending_init_cuids_.empty();
}

// --------------------- Metadata Scan Queue --------------------- //
void HotspotManager::EnqueueMetadataScan(uint64_t cuid) {
  std::lock_guard<std::mutex> lock(pending_metadata_mutex_);
  // 避免重复添加
  for (uint64_t c : pending_metadata_scans_) {
    if (c == cuid) return;
  }
  pending_metadata_scans_.push_back(cuid);
  fprintf(stdout,
          "[HotspotManager] Enqueued CUID %lu for metadata ref-count scan\n",
          cuid);
}

std::vector<uint64_t> HotspotManager::PopPendingMetadataScans() {
  std::lock_guard<std::mutex> lock(pending_metadata_mutex_);
  std::vector<uint64_t> result = std::move(pending_metadata_scans_);
  pending_metadata_scans_.clear();
  return result;
}

bool HotspotManager::HasPendingMetadataScans() const {
  std::lock_guard<std::mutex> lock(pending_metadata_mutex_);
  return !pending_metadata_scans_.empty();
}

// --------------------- Partial Merge Queue --------------------- //

void HotspotManager::EnqueuePartialMerge(uint64_t cuid,
                                         const std::string& scan_first_key,
                                         const std::string& scan_last_key) {
  std::lock_guard<std::mutex> lock(partial_merge_mutex_);
  // 避免重复添加相同 cuid 的任务
  for (const auto& task : partial_merge_queue_) {
    if (task.cuid == cuid) return;
  }
  partial_merge_queue_.push_back({cuid, scan_first_key, scan_last_key});
  fprintf(
      stdout,
      "[HotspotManager] Enqueued PartialMerge for CUID %lu, range [%zu, %zu]\n",
      cuid, scan_first_key.size(), scan_last_key.size());
}

void HotspotManager::ProcessPartialMerge() {
  PartialMergePendingTask task;
  {
    std::lock_guard<std::mutex> lock(partial_merge_mutex_);
    if (partial_merge_queue_.empty()) return;
    task = std::move(partial_merge_queue_.front());
    partial_merge_queue_.pop_front();
  }

  // 获取重叠的 segments
  std::vector<DataSegment> overlapping_snaps, overlapping_deltas;
  index_table_.GetOverlappingSegments(task.cuid, task.scan_first_key,
                                      task.scan_last_key, &overlapping_snaps,
                                      &overlapping_deltas);

  // 如果没有重叠数据，使用 FullReplace 逻辑
  if (overlapping_snaps.empty() && overlapping_deltas.empty()) {
    FinalizeScanAsCompaction(task.cuid);
    return;
  }

  // TODO: 实现完整的归并逻辑
  // 1. 创建 HotSnapshotIterator 读取 overlapping_snaps
  // 2. 创建 HotDeltaIterator 读取 overlapping_deltas
  // 3. 创建 BufferIterator 读取新扫描数据
  // 4. 使用 MergingIterator 归并三路
  // 5. 写入新 SST
  // 6. 调用 ReplaceOverlappingSegments()

  fprintf(stdout,
          "[HotspotManager] ProcessPartialMerge for CUID %lu: "
          "found %zu overlapping snapshots, %zu overlapping deltas\n",
          task.cuid, overlapping_snaps.size(), overlapping_deltas.size());

  // 暂时使用 FullReplace 作为 fallback
  FinalizeScanAsCompaction(task.cuid);
}

bool HotspotManager::HasPendingPartialMerge() const {
  std::lock_guard<std::mutex> lock(partial_merge_mutex_);
  return !partial_merge_queue_.empty();
}

bool HotspotManager::PopPendingPartialMerge(PartialMergePendingTask* task) {
  std::lock_guard<std::mutex> lock(partial_merge_mutex_);
  if (partial_merge_queue_.empty()) {
    return false;
  }
  *task = std::move(partial_merge_queue_.front());
  partial_merge_queue_.pop_front();
  return true;
}

}  // namespace ROCKSDB_NAMESPACE