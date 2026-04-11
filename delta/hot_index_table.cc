#include "delta/hot_index_table.h"

#include "util/extract_cuid.h"

namespace ROCKSDB_NAMESPACE {

HotIndexTable::HotIndexTable(
    const InternalKeyComparator* icmp,
    std::shared_ptr<HotSstLifecycleManager> lifecycle_manager,
    std::shared_ptr<Logger> info_log, size_t num_shards)
    : icmp_(icmp),
      shards_(num_shards),
      lifecycle_manager_(lifecycle_manager),
      info_log_(info_log) {}

// Helper to extract row ID from key for gap detection
static uint64_t ExtractRowID(const std::string& key) {
  if (key.size() < 34) return 0;
  try {
    return std::stoull(key.substr(24, 10));
  } catch (...) {
    return 0;
  }
}

static bool DetectSnapshotGap(uint64_t cuid, const std::vector<DataSegment>& segments, const char* context) {
  if (segments.size() < 2) return false;
  for (size_t i = 0; i + 1 < segments.size(); ++i) {
    uint64_t last_id = ExtractRowID(segments[i].last_key);
    uint64_t next_id = ExtractRowID(segments[i+1].first_key);
    if (last_id != 0 && next_id != 0 && last_id + 1 != next_id && last_id < next_id) {
      fprintf(stderr, "[DIAG_GAP_DETECTED] CUID %lu [%s] GAP between Seg%zu and Seg%zu! "
              "Last (File %lu): %lu, Next (File %lu): %lu. Missing: %lu\n",
              cuid, context, i, i+1, segments[i].file_number, last_id, 
              segments[i+1].file_number, next_id, last_id + 1);
      for (size_t j = 0; j < segments.size(); ++j) {
        fprintf(stderr, "  Seg%zu: File %lu, Range [%s, %s]\n", j, segments[j].file_number,
                FormatKeyDisplay(segments[j].first_key).c_str(),
                FormatKeyDisplay(segments[j].last_key).c_str());
      }
      return true;
    }
  }
  return false;
}

static bool HasActualDataInBuffer(uint64_t cuid, const std::string& start,
                                  const std::string& end, HotDataBuffer* buffer,
                                  const InternalKeyComparator* icmp) {
  if (icmp->Compare(start, end) >= 0) return false;

  std::unique_ptr<InternalIterator> iter(buffer->NewIterator(cuid, icmp));
  iter->Seek(start);

  // 如果找到的第一个 Key 已经超过了范围终点，则说明区间内无数据
  return iter->Valid() && icmp->Compare(iter->key(), end) < 0;
}

void HotIndexTable::UpdateSnapshot(
    uint64_t cuid, const std::vector<DataSegment>& new_segments) {
  std::vector<DataSegment> segments_to_unref;
  {
    Shard& shard = GetShard(cuid);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    auto& entry = shard.table[cuid];
    // Old Snapshot
    if (entry.HasSnapshot()) {
      // 检测新 snapshot 是否有 GAP，有则打印完整上下文
      if (DetectSnapshotGap(cuid, new_segments, "UpdateSnapshot")) {
        fprintf(stderr, "[DIAG][UpdateSnapshot] CUID %lu: %zu old segs -> %zu new segs (GAP above)\n",
                cuid, entry.snapshot_segments.size(), new_segments.size());
        for (size_t i = 0; i < entry.snapshot_segments.size(); ++i) {
          fprintf(stderr, "  old[%zu] file=%lu [%s - %s]\n", i,
                  entry.snapshot_segments[i].file_number,
                  FormatKeyDisplay(entry.snapshot_segments[i].first_key).c_str(),
                  FormatKeyDisplay(entry.snapshot_segments[i].last_key).c_str());
        }
        for (size_t i = 0; i < new_segments.size(); ++i) {
          fprintf(stderr, "  new[%zu] file=%lu [%s - %s]\n", i,
                  new_segments[i].file_number,
                  FormatKeyDisplay(new_segments[i].first_key).c_str(),
                  FormatKeyDisplay(new_segments[i].last_key).c_str());
        }
      }
      segments_to_unref.insert(segments_to_unref.end(),
                               entry.snapshot_segments.begin(),
                               entry.snapshot_segments.end());
    }
    entry.snapshot_segments = new_segments;
  }

  if (lifecycle_manager_) {
    for (const auto& seg : new_segments) {
      lifecycle_manager_->Ref(seg.file_number);
    }
    for (const auto& seg : segments_to_unref) {
      lifecycle_manager_->Unref(seg.file_number);
    }
  }
}

void HotIndexTable::AppendSnapshotSegment(uint64_t cuid,
                                          const DataSegment& segment) {
  fprintf(stderr, "[DIAG][AppendSnapshotSegment] CUID %lu: file=%lu [%s - %s]\n",
          cuid, segment.file_number,
          FormatKeyDisplay(segment.first_key).c_str(),
          FormatKeyDisplay(segment.last_key).c_str());
  {
    Shard& shard = GetShard(cuid);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    auto& segments = shard.table[cuid].snapshot_segments;
    // 使用排序插入而非 push_back。
    // 当 FinalizePmPendingSnapshots 有多个 pm_pending SST 时，
    // 第一个 SST 消耗了 -1 段（right remnant 被 discarded），后续 SST
    // 找不到 -1 段而走此路径。若 snapshot 中还有 PM 范围之后的段（Seg-C），
    // 直接 push_back 会使 SST 排在 Seg-C 之后，造成乱序。
    auto pos = std::lower_bound(
        segments.begin(), segments.end(), segment,
        [this](const DataSegment& a, const DataSegment& b) {
          return icmp_->Compare(a.first_key, b.first_key) < 0;
        });
    segments.insert(pos, segment);
  }

  if (lifecycle_manager_) {
    lifecycle_manager_->Ref(segment.file_number);
  }
}

bool HotIndexTable::PromoteSnapshot(uint64_t cuid,
                                    const DataSegment& new_segment,
                                    HotDataBuffer* buffer,
                                    const InternalKeyComparator* icmp) {
  Shard& shard = GetShard(cuid);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  if (it == shard.table.end()) {
    return false;
  }

  auto& entry = it->second;

  bool found_mem_overlap = false;   // 是否找到了可替换的 -1 内存段
  bool found_phys_overlap = false;  // 是否有物理段与新 SST 重叠
  std::vector<DataSegment> next_segments;
  std::vector<DataSegment> clipped_mem_segments; // 记录被裁减的 mem 段

  std::string new_start_user = ExtractUserKey(new_segment.first_key).ToString();
  std::string new_end_user = ExtractUserKey(new_segment.last_key).ToString();

  for (const auto& seg : entry.snapshot_segments) {
    std::string seg_start_user = ExtractUserKey(seg.first_key).ToString();
    std::string seg_end_user = ExtractUserKey(seg.last_key).ToString();

    // 检查区间重叠 (UserKey)
    bool is_left =
        (icmp->user_comparator()->Compare(seg_end_user, new_start_user) < 0);
    bool is_right =
        (icmp->user_comparator()->Compare(seg_start_user, new_end_user) > 0);
    bool overlaps = !is_left && !is_right;

    if (overlaps) {
      if (seg.file_number != static_cast<uint64_t>(-1)) {
        // [IMPORTANT]: NEVER cut a physical snapshot segment.
        // Physical segments contain dense historical data that may be needed by
        // older MVCC readers. The new SST from buffer flush may be sparse
        // (cross-scan contamination) and must not destroy dense data.
        next_segments.push_back(seg);
        found_phys_overlap = true;
        continue;
      }

      found_mem_overlap = true;  // found a -1 segment to replace
      clipped_mem_segments.push_back(seg);

      // 只针对 Memory 段 (-1) 进行切分
      // 1. 左侧剩余: [seg.first, new.first)
      if (icmp->Compare(seg.first_key, new_segment.first_key) < 0) {
        bool keep = HasActualDataInBuffer(cuid, seg.first_key,
                                          new_segment.first_key, buffer, icmp);
        if (keep) {
          DataSegment left = seg;
          left.last_key = new_segment.first_key;
          next_segments.push_back(left);
        } else {
          fprintf(stderr, "[DIAG_CLIP_LEFT] CUID %lu: Mem Seg left remnant discarded! Range: [%s - %s). Keep = false\n",
                  cuid, FormatKeyDisplay(seg.first_key).c_str(), FormatKeyDisplay(new_segment.first_key).c_str());
        }
      }
      // 2. 右侧剩余: [new.last, seg.last)
      if (icmp->Compare(seg.last_key, new_segment.last_key) > 0) {
        bool keep = HasActualDataInBuffer(cuid, new_segment.last_key,
                                          seg.last_key, buffer, icmp);
        if (keep) {
          DataSegment right = seg;
          right.first_key = new_segment.last_key;
          next_segments.push_back(right);
          fprintf(stderr, "[DIAG][PromoteSnapshot] CUID %lu: RIGHT remnant KEPT [%s - %s)\n",
                  cuid, FormatKeyDisplay(new_segment.last_key).c_str(),
                  FormatKeyDisplay(seg.last_key).c_str());
        } else {
          fprintf(stderr, "[DIAG_CLIP_RIGHT] CUID %lu: Mem Seg right remnant discarded! Range: [%s - %s). Keep = false\n",
                  cuid, FormatKeyDisplay(new_segment.last_key).c_str(), FormatKeyDisplay(seg.last_key).c_str());
        }
      }

      // 3. 填入重叠部分的物理段，严格 clip 到原 mem segment 的边界
      DataSegment replacement = new_segment;
      if (icmp->Compare(seg.first_key, replacement.first_key) > 0) {
        replacement.first_key = seg.first_key;
      }
      if (icmp->Compare(seg.last_key, replacement.last_key) < 0) {
        replacement.last_key = seg.last_key;
      }
      next_segments.push_back(replacement);

      if (false) {
        fprintf(
            stderr,
            "[DIAG_PROMOTE] CUID %lu: Mem Segment [%s - %s] replaced by SST "
            "%lu clipped to [%s - %s]\n",
            cuid, FormatKeyDisplay(seg.first_key).c_str(),
            FormatKeyDisplay(seg.last_key).c_str(), new_segment.file_number,
            FormatKeyDisplay(replacement.first_key).c_str(),
            FormatKeyDisplay(replacement.last_key).c_str());
      }
    } else {
      next_segments.push_back(seg);
    }
  }

  // if (DetectSnapshotGap(cuid, next_segments, "PromoteSnapshot")) {
  // }

  if (found_mem_overlap) {
    std::sort(next_segments.begin(), next_segments.end(),
              [icmp](const DataSegment& a, const DataSegment& b) {
                int cmp = icmp->Compare(a.first_key, b.first_key);
                if (cmp != 0) return cmp < 0;
                return icmp->Compare(a.last_key, b.last_key) < 0;
              });

    // GAP 检测：只在出现间隙时打印上下文
    // if (DetectSnapshotGap(cuid, next_segments, "PromoteSnapshot")) {
    //   fprintf(stderr, "[DIAG][PromoteSnapshot] CUID %lu: GAP! newSST file=%lu [%s - %s]. Clipped mem segs=%zu\n",
    //           cuid, new_segment.file_number,
    //           FormatKeyDisplay(new_segment.first_key).c_str(),
    //           FormatKeyDisplay(new_segment.last_key).c_str(),
    //           clipped_mem_segments.size());
    //   for (size_t i = 0; i < clipped_mem_segments.size(); ++i) {
    //     fprintf(stderr, "  clipped_mem[%zu] [%s - %s]\n", i,
    //             FormatKeyDisplay(clipped_mem_segments[i].first_key).c_str(),
    //             FormatKeyDisplay(clipped_mem_segments[i].last_key).c_str());
    //   }
    //   for (size_t i = 0; i < next_segments.size(); ++i) {
    //     fprintf(stderr, "  result[%zu] file=%lu [%s - %s]\n", i,
    //             next_segments[i].file_number,
    //             FormatKeyDisplay(next_segments[i].first_key).c_str(),
    //             FormatKeyDisplay(next_segments[i].last_key).c_str());
    //   }
    // }

    entry.snapshot_segments = std::move(next_segments);

    if (lifecycle_manager_) {
      lifecycle_manager_->Ref(new_segment.file_number);
    }
  } else if (found_phys_overlap) {
    // 新 SST 与物理段重叠但未被加入 snapshot：记录以便排查数据丢失
    fprintf(stderr, "[DIAG][PromoteSnapshot] CUID %lu: PHYS_OVERLAP_ONLY newSST file=%lu [%s - %s] skipped (no -1 seg matched)\n",
            cuid, new_segment.file_number,
            FormatKeyDisplay(new_segment.first_key).c_str(),
            FormatKeyDisplay(new_segment.last_key).c_str());
  }

  // 返回 true 的条件：找到了可替换的 -1
  // 段，或者虽然只有物理段重叠但我们选择保护它们。 两种情况都不应走到
  // AppendSnapshotSegment 的 fallback 路径。
  return found_mem_overlap || found_phys_overlap;
}

void HotIndexTable::AddDelta(uint64_t cuid, const DataSegment& delta) {
  {
    Shard& shard = GetShard(cuid);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    auto& entry = shard.table[cuid];

    shard.table[cuid].deltas.push_back(delta);
  }

  // if (lifecycle_manager_) {
  //   lifecycle_manager_->Ref(delta.file_number);
  // }
}

bool HotIndexTable::GetEntry(uint64_t cuid, HotIndexEntry* entry) const {
  const Shard& shard = GetShard(cuid);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  if (it != shard.table.end()) {
    if (entry) {
      *entry = it->second;
    }
    return true;
  }
  return false;
}

bool HotIndexTable::GetEntryAndRefSnapshots(uint64_t cuid,
                                            HotIndexEntry* out_entry) const {
  const Shard& shard = GetShard(cuid);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  if (it == shard.table.end()) return false;
  if (out_entry) {
    *out_entry = it->second;
    // 在 shared_lock 持有期间 Ref 所有物理段：
    // 此时写路径的 unique_lock 无法同时持有，保证 Ref 发生在任何
    // AtomicReplaceForPartialMerge 的锁外 Unref 之前。
    if (lifecycle_manager_) {
      for (const auto& seg : out_entry->snapshot_segments) {
        if (seg.file_number != static_cast<uint64_t>(-1)) {
          lifecycle_manager_->Ref(seg.file_number);
        }
      }
    }
  }
  return true;
}

void HotIndexTable::MarkDeltasAsObsolete(
    uint64_t cuid, const std::unordered_set<uint64_t>& visited_files) {
  Shard& shard = GetShard(cuid);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  if (it == shard.table.end()) return;

  auto& entry = it->second;
  if (visited_files.empty()) return;

  // 从 entry.deltas 中匹配并移入 obsolete_deltas
  std::unordered_set<uint64_t> matched_files;
  for (auto d_it = entry.deltas.begin(); d_it != entry.deltas.end();) {
    if (visited_files.count(d_it->file_number)) {
      matched_files.insert(d_it->file_number);
      entry.obsolete_deltas.push_back(*d_it);
      d_it = entry.deltas.erase(d_it);
    } else {
      ++d_it;
    }
  }

  // 对于 visited_files 中不在 deltas 中找到的文件
  // 可能是因为第一次 full scan 之前生成的 SST，直接标记为 obsolete
  // TIPS & TODO：是否存在问题？
  for (uint64_t fid : visited_files) {
    if (matched_files.count(fid)) continue;
    DataSegment dummy;
    dummy.file_number = fid;
    entry.obsolete_deltas.push_back(dummy);
  }
}

// d)
// 若遇到热点CUid，检查其热点索引表，若发现Deltas列表中对应的该段数据已被标记为
// Obsolete， 则直接跳过该段数据，并删除对应的Deltas记录。 Deprecated：不能在
// compaction 时直接清理
bool HotIndexTable::CheckAndRemoveObsoleteDeltas(
    uint64_t cuid, const std::vector<uint64_t>& input_files) {
  Shard& shard = GetShard(cuid);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  if (it == shard.table.end()) return false;

  auto& entry = it->second;
  if (entry.obsolete_deltas.empty()) return false;

  bool found_obsolete = false;

  for (auto d_it = entry.obsolete_deltas.begin();
       d_it != entry.obsolete_deltas.end();) {
    bool is_input_file = false;
    for (uint64_t fid : input_files) {
      if (d_it->file_number == fid) {
        is_input_file = true;
        break;
      }
    }
    if (is_input_file) {
      found_obsolete = true;
      d_it = entry.obsolete_deltas.erase(d_it);
    } else {
      ++d_it;
    }
  }
  return found_obsolete;
}

bool HotIndexTable::IsDeltaObsolete(
    uint64_t cuid, const std::vector<uint64_t>& input_files) const {
  const Shard& shard = GetShard(cuid);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  if (it == shard.table.end()) return false;

  const auto& entry = it->second;
  if (entry.obsolete_deltas.empty()) return false;

  for (const auto& delta : entry.obsolete_deltas) {
    for (uint64_t fid : input_files) {
      if (delta.file_number == fid) {
        return true;
      }
    }
  }
  return false;
}

void HotIndexTable::RemoveObsoleteDeltasForCUIDs(
    const std::unordered_set<uint64_t>& cuids,
    const std::vector<uint64_t>& input_files) {
  std::unordered_set<uint64_t> input_files_set(input_files.begin(),
                                               input_files.end());

  for (uint64_t cuid : cuids) {
    Shard& shard = GetShard(cuid);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    auto entry_it = shard.table.find(cuid);
    if (entry_it == shard.table.end()) continue;

    auto& entry = entry_it->second;

    // 1. 移除 deltas 中的匹配文件
    for (auto it = entry.deltas.begin(); it != entry.deltas.end();) {
      if (input_files_set.count(it->file_number)) {
        it = entry.deltas.erase(it);
      } else {
        ++it;
      }
    }

    // 2. 移除 obsolete_deltas 中的匹配文件
    for (auto it = entry.obsolete_deltas.begin();
         it != entry.obsolete_deltas.end();) {
      if (input_files_set.count(it->file_number)) {
        it = entry.obsolete_deltas.erase(it);
      } else {
        ++it;
      }
    }
  }
}

// L0Compaction更新 Delta 索引
// remove delta and add new data
void HotIndexTable::UpdateDeltaIndex(uint64_t cuid,
                                     const std::vector<uint64_t>& input_files,
                                     const DataSegment& new_delta) {
  Shard& shard = GetShard(cuid);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto& entry = shard.table[cuid];

  // 清理本次 Compaction 涉及的文件
  for (auto it = entry.deltas.begin(); it != entry.deltas.end();) {
    bool is_input = false;
    for (uint64_t fid : input_files) {
      if (it->file_number == fid) {
        is_input = true;
        break;
      }
    }

    if (is_input) {
      it = entry.deltas.erase(it);
    } else {
      ++it;
    }
  }

  // double check obsolete_deltas
  for (auto it = entry.obsolete_deltas.begin();
       it != entry.obsolete_deltas.end();) {
    bool is_input = false;
    for (uint64_t fid : input_files) {
      if (it->file_number == fid) {
        is_input = true;
        break;
      }
    }
    if (is_input) {
      it = entry.obsolete_deltas.erase(it);
    } else {
      ++it;
    }
  }

  // 加入 new delta index
  entry.deltas.push_back(new_delta);
}

void HotIndexTable::RemoveCUID(uint64_t cuid) {
  std::vector<DataSegment> segments_to_unref;
  {
    Shard& shard = GetShard(cuid);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    auto it = shard.table.find(cuid);
    if (it != shard.table.end()) {
      // Snapshot 片段
      if (it->second.HasSnapshot()) {
        segments_to_unref.insert(segments_to_unref.end(),
                                 it->second.snapshot_segments.begin(),
                                 it->second.snapshot_segments.end());
      }
      shard.table.erase(it);
    }
  }

  // Unref
  if (lifecycle_manager_) {
    for (const auto& seg : segments_to_unref) {
      lifecycle_manager_->Unref(seg.file_number);
    }
  }
}

void HotIndexTable::ReplaceOverlappingSegments(
    uint64_t cuid, const DataSegment& new_segment,
    const std::vector<uint64_t>& obsolete_delta_files) {
  std::vector<DataSegment> segments_to_unref;

  {
    Shard& shard = GetShard(cuid);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    auto& entry = shard.table[cuid];
    auto& snapshots = entry.snapshot_segments;

    std::vector<DataSegment> next_segments;

    std::string new_start_user =
        ExtractUserKey(new_segment.first_key).ToString();
    std::string new_end_user = ExtractUserKey(new_segment.last_key).ToString();

    // 判定与new snapshot相关的snapshot -> !(End < New.Start || Start > New.End)
    // 先将原先的 snapshots 进行分割+排序
    for (auto it = snapshots.begin(); it != snapshots.end(); ++it) {
      std::string seg_start_user = ExtractUserKey(it->first_key).ToString();
      std::string seg_end_user = ExtractUserKey(it->last_key).ToString();

      bool is_left =
          (icmp_->user_comparator()->Compare(seg_end_user, new_start_user) < 0);
      bool is_right =
          (icmp_->user_comparator()->Compare(seg_start_user, new_end_user) > 0);

      if (!is_left && !is_right) {
        // 重叠的情况: 无论物理段还是内存段，都保留不重叠的边缘部分（防止 scope
        // 缩小导致数据丢失）
        // 1. 左侧剩余部分
        if (icmp_->user_comparator()->Compare(seg_start_user, new_start_user) <
            0) {
          DataSegment left_seg = *it;
          left_seg.last_key = new_segment.first_key;
          next_segments.push_back(left_seg);
          if (it->file_number != static_cast<uint64_t>(-1) &&
              lifecycle_manager_) {
            lifecycle_manager_->Ref(it->file_number);
          }
        }
        // 2. 右侧剩余部分
        if (icmp_->user_comparator()->Compare(seg_end_user, new_end_user) > 0) {
          DataSegment right_seg = *it;
          right_seg.first_key = new_segment.last_key;
          next_segments.push_back(right_seg);
          if (it->file_number != static_cast<uint64_t>(-1) &&
              lifecycle_manager_) {
            lifecycle_manager_->Ref(it->file_number);
          }
        }

        // 原来的段如果包含物理文件，则需要放入 unref 列表
        if (it->file_number != static_cast<uint64_t>(-1)) {
          segments_to_unref.push_back(*it);
        }
      } else {
        // 不重叠，保留原样
        next_segments.push_back(*it);
      }
    }

    // 检测单 Key 分段的来源
    if (icmp_->user_comparator()->Compare(
            ExtractUserKey(new_segment.first_key),
            ExtractUserKey(new_segment.last_key)) == 0) {
      fprintf(stderr,
              "[DIAG_SINGLE_KEY_SEG] CUID %lu: New single-key segment [%s] "
              "added.\n",
              cuid, FormatKeyDisplay(new_segment.first_key).c_str());
    }

    next_segments.push_back(new_segment);

    // 重新排序 (使用 InternalKeyComparator)
    std::sort(next_segments.begin(), next_segments.end(),
              [this](const DataSegment& a, const DataSegment& b) {
                int cmp = icmp_->Compare(a.first_key, b.first_key);
                if (cmp != 0) return cmp < 0;
                return icmp_->Compare(a.last_key, b.last_key) < 0;
              });
    
    if (DetectSnapshotGap(cuid, next_segments, "ReplaceOverlappingSegments")) {
      fprintf(stderr, "[DIAG][ReplaceOverlapping] CUID %lu: GAP! new={file=%lu [%s - %s]}\n",
              cuid, new_segment.file_number,
              FormatKeyDisplay(new_segment.first_key).c_str(),
              FormatKeyDisplay(new_segment.last_key).c_str());
      for (size_t i = 0; i < next_segments.size(); ++i) {
        fprintf(stderr, "  result[%zu] file=%lu [%s - %s]\n", i,
                next_segments[i].file_number,
                FormatKeyDisplay(next_segments[i].first_key).c_str(),
                FormatKeyDisplay(next_segments[i].last_key).c_str());
      }
      for (size_t j = 0; j < entry.deltas.size(); ++j) {
        fprintf(stderr, "  delta[%zu] file=%lu [%s - %s]\n", j,
                entry.deltas[j].file_number,
                FormatKeyDisplay(entry.deltas[j].first_key).c_str(),
                FormatKeyDisplay(entry.deltas[j].last_key).c_str());
      }
    }
    // DetectSnapshotGap(cuid, snapshots, "ReplaceOverlappingSegments");

    snapshots = std::move(next_segments);

    // 插入这个新的 buffer snapshot，重新寻找插入位置进行合并相邻的内存段 (-1)
    auto insert_pos = std::find_if(
        snapshots.begin(), snapshots.end(), [&](const DataSegment& s) {
          return s.first_key == new_segment.first_key &&
                 s.last_key == new_segment.last_key &&
                 s.file_number == static_cast<uint64_t>(-1);
        });

    // 合并相邻的内存段 (-1)，因为之前切分+排序插入过了
    // 直接检查前后buffer segment，如果存在就合并成一个更大的 segment
    // MARK：多个 buffer segment 的情况？
    if (insert_pos != snapshots.end() &&
        insert_pos->file_number == static_cast<uint64_t>(-1)) {
      // 检查后一个
      auto next = std::next(insert_pos);
      if (next != snapshots.end()) {
        if (next->file_number == static_cast<uint64_t>(-1)) {
          insert_pos->last_key = next->last_key;
          snapshots.erase(next);
        }
      }
      // 检查前一个
      if (insert_pos != snapshots.begin()) {
        auto prev = std::prev(insert_pos);
        if (prev->file_number == static_cast<uint64_t>(-1)) {
          prev->last_key = insert_pos->last_key;
          snapshots.erase(insert_pos);
        }
      }
    }

    if (!obsolete_delta_files.empty()) {
      auto& deltas = entry.deltas;

      // 遍历 obsolete_delta_files
      for (uint64_t file_num : obsolete_delta_files) {
        // 在 entry.deltas 中找到并移除，同时移入 obsolete_deltas
        for (auto it = deltas.begin(); it != deltas.end();) {
          if (it->file_number == file_num) {
            // 记录到 Obsolete (L0 Compaction再清除)
            entry.obsolete_deltas.push_back(*it);
            it = deltas.erase(it);
            // 注意：一个 file_number 在 deltas 中可能出现多次吗？
          } else {
            ++it;
          }
        }
      }
    }
  }  // Unlock

  if (lifecycle_manager_) {
    if (new_segment.file_number != static_cast<uint64_t>(-1)) {
      lifecycle_manager_->Ref(new_segment.file_number);
    }
    for (const auto& seg : segments_to_unref) {
      lifecycle_manager_->Unref(seg.file_number);
    }
  }
}

void HotIndexTable::AtomicReplaceForPartialMerge(
    uint64_t cuid,
    const std::string& pm_range_first,
    const std::string& pm_range_last,
    const std::vector<DataSegment>& pm_sst_segs,
    bool has_buf_data,
    const std::string& buf_min,
    const std::string& buf_max,
    const std::vector<uint64_t>& obsolete_delta_files) {
  // 在 shard lock 外收集待 Unref 的旧段（避免锁内调用 lifecycle_manager_→死锁风险）
  std::vector<DataSegment> segments_to_unref;

  {
    Shard& shard = GetShard(cuid);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    auto& entry = shard.table[cuid];
    auto& snapshots = entry.snapshot_segments;

    std::string pm_start_user = ExtractUserKey(pm_range_first).ToString();
    std::string pm_end_user   = ExtractUserKey(pm_range_last).ToString();

    std::vector<DataSegment> next_segments;

    // ① 裁剪与 PM 输出范围重叠的旧 snapshot 段（逻辑与 ReplaceOverlappingSegments 相同）
    for (const auto& seg : snapshots) {
      std::string seg_start = ExtractUserKey(seg.first_key).ToString();
      std::string seg_end   = ExtractUserKey(seg.last_key).ToString();

      bool is_left  = (icmp_->user_comparator()->Compare(seg_end,   pm_start_user) < 0);
      bool is_right = (icmp_->user_comparator()->Compare(seg_start, pm_end_user)   > 0);

      if (!is_left && !is_right) {
        // 与 PM 范围重叠：保留不重叠的边缘部分
        // 1. 左侧剩余
        if (icmp_->user_comparator()->Compare(seg_start, pm_start_user) < 0) {
          DataSegment left = seg;
          left.last_key = pm_range_first;
          next_segments.push_back(left);
          // 左残余与原始段共享文件，额外 Ref
          if (seg.file_number != static_cast<uint64_t>(-1) && lifecycle_manager_) {
            lifecycle_manager_->Ref(seg.file_number);
          }
        }
        // 2. 右侧剩余
        if (icmp_->user_comparator()->Compare(seg_end, pm_end_user) > 0) {
          DataSegment right = seg;
          right.first_key = pm_range_last;
          next_segments.push_back(right);
          if (seg.file_number != static_cast<uint64_t>(-1) && lifecycle_manager_) {
            lifecycle_manager_->Ref(seg.file_number);
          }
        }
        // 原始段收集待 Unref（物理段需要减少 snapshot Ref）
        if (seg.file_number != static_cast<uint64_t>(-1)) {
          segments_to_unref.push_back(seg);
        }
      } else {
        // 不重叠：原样保留
        next_segments.push_back(seg);
      }
    }

    // ② 插入 pm_sst_segs（pm_pending Ref 就地转移为 snapshot Ref，不额外 Ref）
    // 同时移除 next_segments 中被 pm_sst_seg 覆盖的旧 -1 段：
    // -1 段的 buffer 数据已被 flush 生成了此 SST，-1 段需被 SST 替代，否则
    // 读者访问旧 -1 段时 buffer 已无数据，造成 kSnapshotChanged
    for (const auto& sst_seg : pm_sst_segs) {
      // pm_sst_seg 来自 buffer flush，其实际 first/last_key 可能超出 pm_range
      // （buffer 中可能含有来自普通写入的、pm_range 范围之外的 CUID 数据）。
      // 必须将其 clip 到 pm_range，防止与 step① 保留的 pm_range 之外的物理段重叠。
      // 逻辑与 PromoteSnapshot 将 SST clip 到 -1 段边界完全对称。
      DataSegment clipped_seg = sst_seg;
      if (icmp_->user_comparator()->Compare(
              ExtractUserKey(clipped_seg.first_key), pm_start_user) < 0) {
        clipped_seg.first_key = pm_range_first;
      }
      if (icmp_->user_comparator()->Compare(
              ExtractUserKey(clipped_seg.last_key), pm_end_user) > 0) {
        clipped_seg.last_key = pm_range_last;
      }

      // Clip 后若范围变为空（SST 完全位于 pm_range 之外），释放 Ref 并跳过
      if (icmp_->user_comparator()->Compare(
              ExtractUserKey(clipped_seg.first_key),
              ExtractUserKey(clipped_seg.last_key)) > 0) {
        if (lifecycle_manager_) {
          lifecycle_manager_->Unref(sst_seg.file_number);
        }
        continue;
      }

      std::string sst_start = ExtractUserKey(clipped_seg.first_key).ToString();
      std::string sst_end   = ExtractUserKey(clipped_seg.last_key).ToString();

      std::vector<DataSegment> after_promote;
      for (const auto& seg : next_segments) {
        if (seg.file_number != static_cast<uint64_t>(-1)) {
          // 物理段不受影响，原样保留
          after_promote.push_back(seg);
          continue;
        }
        // -1 段：检查是否与 sst_seg 重叠
        std::string seg_start = ExtractUserKey(seg.first_key).ToString();
        std::string seg_end   = ExtractUserKey(seg.last_key).ToString();
        bool is_left  = icmp_->user_comparator()->Compare(seg_end,   sst_start) < 0;
        bool is_right = icmp_->user_comparator()->Compare(seg_start, sst_end)   > 0;
        if (is_left || is_right) {
          // 不重叠，保留
          after_promote.push_back(seg);
        } else {
          // 重叠：-1 段被 SST 替代，保留左/右残余（-1 段无需 Unref，file_number=-1）
          if (icmp_->user_comparator()->Compare(seg_start, sst_start) < 0) {
            DataSegment left = seg;
            left.last_key = clipped_seg.first_key;
            after_promote.push_back(left);
          }
          if (icmp_->user_comparator()->Compare(seg_end, sst_end) > 0) {
            DataSegment right = seg;
            right.first_key = clipped_seg.last_key;
            after_promote.push_back(right);
          }
          // 中间 -1 段（被 SST 完全替代）直接丢弃，无 Ref/Unref 操作
        }
      }
      next_segments = std::move(after_promote);
      next_segments.push_back(clipped_seg);
    }

    // ③ 若 buffer 有剩余数据，插入 -1 段覆盖实际 buffer 范围
    if (has_buf_data) {
      DataSegment buf_seg;
      buf_seg.file_number = static_cast<uint64_t>(-1);
      buf_seg.first_key   = buf_min;
      buf_seg.last_key    = buf_max;
      next_segments.push_back(buf_seg);
    }

    // ④ 排序
    std::sort(next_segments.begin(), next_segments.end(),
              [this](const DataSegment& a, const DataSegment& b) {
                int cmp = icmp_->Compare(a.first_key, b.first_key);
                if (cmp != 0) return cmp < 0;
                return icmp_->Compare(a.last_key, b.last_key) < 0;
              });

    // ④ 合并相邻 -1 段（排序后相邻 -1 直接合并）
    std::vector<DataSegment> merged;
    for (auto& s : next_segments) {
      if (!merged.empty() &&
          merged.back().file_number == static_cast<uint64_t>(-1) &&
          s.file_number == static_cast<uint64_t>(-1)) {
        if (icmp_->Compare(s.last_key, merged.back().last_key) > 0) {
          merged.back().last_key = s.last_key;
        }
      } else {
        merged.push_back(std::move(s));
      }
    }
    next_segments = std::move(merged);

    // GAP 诊断
    if (DetectSnapshotGap(cuid, next_segments, "AtomicReplaceForPartialMerge")) {
      fprintf(stderr,
              "[DIAG][AtomicReplace] CUID %lu: GAP! pm_range=[%s - %s] "
              "has_buf=%d pm_ssts=%zu\n",
              cuid,
              FormatKeyDisplay(pm_range_first).c_str(),
              FormatKeyDisplay(pm_range_last).c_str(),
              (int)has_buf_data, pm_sst_segs.size());
      for (size_t i = 0; i < next_segments.size(); ++i) {
        fprintf(stderr, "  result[%zu] file=%lu [%s - %s]\n", i,
                next_segments[i].file_number,
                FormatKeyDisplay(next_segments[i].first_key).c_str(),
                FormatKeyDisplay(next_segments[i].last_key).c_str());
      }
    }

    snapshots = std::move(next_segments);

    // ⑤ 处理 obsolete_delta_files
    if (!obsolete_delta_files.empty()) {
      auto& deltas = entry.deltas;
      for (uint64_t file_num : obsolete_delta_files) {
        for (auto it = deltas.begin(); it != deltas.end();) {
          if (it->file_number == file_num) {
            entry.obsolete_deltas.push_back(*it);
            it = deltas.erase(it);
          } else {
            ++it;
          }
        }
      }
    }
  }  // shard.mutex 

  // ⑥ 锁外释放旧段持有的 snapshot Ref（不影响新 snapshot 的可见性）
  if (lifecycle_manager_) {
    for (const auto& seg : segments_to_unref) {
      lifecycle_manager_->Unref(seg.file_number);
    }
  }
}

size_t HotIndexTable::CountOverlappingDeltas(
    uint64_t cuid, const std::string& first_key,
    const std::string& last_key) const {
  const Shard& shard = GetShard(cuid);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  if (it == shard.table.end()) return 0;

  const auto& entry = it->second;
  size_t count = 0;

  for (const auto& delta : entry.deltas) {
    // Key 范围重叠判断: !(delta.last < first || delta.first > last)
    bool is_left =
        (icmp_->user_comparator()->Compare(ExtractUserKey(delta.last_key),
                                           ExtractUserKey(first_key)) < 0);
    bool is_right =
        (icmp_->user_comparator()->Compare(ExtractUserKey(delta.first_key),
                                           ExtractUserKey(last_key)) > 0);
    if (!is_left && !is_right) {
      count++;
    }
  }
  return count;
}

void HotIndexTable::ClearAllForCuid(uint64_t cuid) {
  std::vector<DataSegment> segments_to_unref;
  {
    Shard& shard = GetShard(cuid);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    auto it = shard.table.find(cuid);
    if (it == shard.table.end()) return;

    auto& entry = it->second;

    // 收集需要 Unref 的 Snapshot 段
    for (const auto& seg : entry.snapshot_segments) {
      if (seg.file_number != static_cast<uint64_t>(-1)) {
        segments_to_unref.push_back(seg);
      }
    }

    // 清空所有数据
    entry.snapshot_segments.clear();
    entry.deltas.clear();
    entry.obsolete_deltas.clear();
  }

  // Unref lifecycle
  if (lifecycle_manager_) {
    for (const auto& seg : segments_to_unref) {
      lifecycle_manager_->Unref(seg.file_number);
    }
  }
}

void HotIndexTable::GetOverlappingSegments(
    uint64_t cuid, const std::string& first_key, const std::string& last_key,
    std::vector<DataSegment>* overlapping_snapshots,
    std::vector<DataSegment>* overlapping_deltas) {
  const Shard& shard = GetShard(cuid);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  if (it == shard.table.end()) return;

  const auto& entry = it->second;

  // 收集重叠的 snapshot segments
  for (const auto& seg : entry.snapshot_segments) {
    // !(seg.last < first || seg.first > last)
    bool is_left =
        (icmp_->user_comparator()->Compare(ExtractUserKey(seg.last_key),
                                           ExtractUserKey(first_key)) < 0);
    bool is_right =
        (icmp_->user_comparator()->Compare(ExtractUserKey(seg.first_key),
                                           ExtractUserKey(last_key)) > 0);
    if (!is_left && !is_right) {
      overlapping_snapshots->push_back(seg);
    }
  }

  // 收集重叠的 delta segments
  for (const auto& seg : entry.deltas) {
    bool is_left =
        (icmp_->user_comparator()->Compare(ExtractUserKey(seg.last_key),
                                           ExtractUserKey(first_key)) < 0);
    bool is_right =
        (icmp_->user_comparator()->Compare(ExtractUserKey(seg.first_key),
                                           ExtractUserKey(last_key)) > 0);
    if (!is_left && !is_right) {
      overlapping_deltas->push_back(seg);
    }
  }
}

void HotIndexTable::DumpToFile(const std::string& filename,
                               const std::string& phase_label) {
  std::ofstream outfile;
  outfile.open(filename, std::ios_base::app);

  if (!outfile.is_open()) {
    std::cerr << "Error: Unable to open dump file: " << filename << std::endl;
    return;
  }

  outfile << "========================================" << std::endl;
  outfile << "DUMP PHASE: " << phase_label << std::endl;

  size_t total_cuids = 0;
  for (size_t i = 0; i < shards_.size(); ++i) {
    std::shared_lock<std::shared_mutex> lock(shards_[i].mutex);
    total_cuids += shards_[i].table.size();
  }
  outfile << "Total Tracked CUIDs: " << total_cuids << std::endl;

  for (size_t i = 0; i < shards_.size(); ++i) {
    std::shared_lock<std::shared_mutex> lock(shards_[i].mutex);
    for (const auto& kv : shards_[i].table) {
      uint64_t cuid = kv.first;
      const HotIndexEntry& entry = kv.second;

      outfile << "CUID: " << cuid << std::endl;

      if (entry.snapshot_segments.empty()) {
        outfile << "  [Snapshot]: None" << std::endl;
      } else {
        outfile << "  [Snapshot]: " << entry.snapshot_segments.size()
                << " segments" << std::endl;
        for (const auto& seg : entry.snapshot_segments) {
          outfile << "    -> FileID: " << (int64_t)seg.file_number
                  << " | FirstKey: " << seg.first_key
                  << " | LastKey: " << seg.last_key;
          if (seg.file_number == (uint64_t)-1) outfile << " (Mem)";
          outfile << std::endl;
        }
      }

      if (entry.deltas.empty()) {
        outfile << "  [Deltas]: None" << std::endl;
      } else {
        outfile << "  [Deltas]: " << entry.deltas.size() << " segments"
                << std::endl;
        for (const auto& seg : entry.deltas) {
          outfile << "    -> FileID: " << seg.file_number
                  << " | FirstKey: " << seg.first_key
                  << " | LastKey: " << seg.last_key << std::endl;
        }
      }

      if (!entry.obsolete_deltas.empty()) {
        outfile << "  [Obsolete]: " << entry.obsolete_deltas.size()
                << " pending cleanup" << std::endl;
        for (const auto& seg : entry.obsolete_deltas) {
          outfile << "    -> FileID: " << seg.file_number << " (Obs)"
                  << std::endl;
        }
      }
    }
  }
  outfile << "----------------------------------------" << std::endl
          << std::endl;
  outfile.close();
}

}  // namespace ROCKSDB_NAMESPACE