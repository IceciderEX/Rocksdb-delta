#include "delta/hot_index_table.h"

namespace ROCKSDB_NAMESPACE {

void HotIndexTable::UpdateSnapshot(
    uint64_t cuid, const std::vector<DataSegment>& new_segments) {
  std::vector<DataSegment> segments_to_unref;
  {
    Shard& shard = GetShard(cuid);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    auto& entry = shard.table[cuid];
    // Old Snapshot
    if (entry.HasSnapshot()) {
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
  {
    Shard& shard = GetShard(cuid);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    shard.table[cuid].snapshot_segments.push_back(segment);
  }

  if (lifecycle_manager_) {
    lifecycle_manager_->Ref(segment.file_number);
  }
}

bool HotIndexTable::PromoteSnapshot(uint64_t cuid,
                                    const DataSegment& new_segment) {
  Shard& shard = GetShard(cuid);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  if (it == shard.table.end()) {
    return false;
  }

  auto& entry = it->second;
  bool found_overlap = false;
  std::vector<DataSegment> next_segments;

  std::string new_start = ExtractUserKey(new_segment.first_key).ToString();
  std::string new_end = ExtractUserKey(new_segment.last_key).ToString();

  // 找到标记为 -1 的内存段，判断是否有重叠，进行 segmentation
  for (const auto& seg : entry.snapshot_segments) {
    if (seg.file_number == static_cast<uint64_t>(-1)) {
      std::string seg_start = ExtractUserKey(seg.first_key).ToString();
      std::string seg_end = ExtractUserKey(seg.last_key).ToString();

      bool is_left = new_end < seg_start;
      bool is_right = new_start > seg_end;

      if (!is_left && !is_right) {
        found_overlap = true;
        // 1. 左侧剩余部分
        if (seg_start < new_start) {
          DataSegment left_seg = seg;
          left_seg.last_key = new_segment.first_key;
          next_segments.push_back(left_seg);
        }

        // 2. 右侧剩余部分
        if (seg_end > new_end) {
          DataSegment right_seg = seg;
          right_seg.first_key = new_segment.last_key;
          next_segments.push_back(right_seg);
        }
      } else {
        next_segments.push_back(seg); // 
      }
    } else {
      next_segments.push_back(seg);
    }
  }

  if (found_overlap) {
    next_segments.push_back(new_segment);
    // 重新排序
    std::sort(next_segments.begin(), next_segments.end(),
              [](const DataSegment& a, const DataSegment& b) {
                return a.first_key < b.first_key;
              });
    entry.snapshot_segments = std::move(next_segments);

    if (lifecycle_manager_) {
      lifecycle_manager_->Ref(new_segment.file_number);
    }
  }

  return found_overlap;
}

void HotIndexTable::AddDelta(uint64_t cuid, const DataSegment& delta) {
  {
    Shard& shard = GetShard(cuid);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
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
    if (entry.obsolete_deltas.empty()) continue;

    // 移除 matched 文件
    for (auto it = entry.obsolete_deltas.begin();
         it != entry.obsolete_deltas.end();) {
      bool is_input = false;
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

    std::string new_start = ExtractUserKey(new_segment.first_key).ToString();
    std::string new_end = ExtractUserKey(new_segment.last_key).ToString();

    // 判定与new snapshot相关的snapshot -> !(End < New.Start || Start > New.End)
    // 先将原先的 snapshots 进行分割+排序
    for (auto it = snapshots.begin(); it != snapshots.end(); ++it) {
      std::string seg_start = ExtractUserKey(it->first_key).ToString();
      std::string seg_end = ExtractUserKey(it->last_key).ToString();

      bool is_left = (seg_end < new_start);
      bool is_right = (seg_start > new_end);

      if (!is_left && !is_right) {
        // 重叠的情况: 无论物理段还是内存段，都保留不重叠的边缘部分（防止 scope 缩小导致数据丢失）
        // 1. 左侧剩余部分
        if (seg_start < new_start) {
          DataSegment left_seg = *it;
          left_seg.last_key = new_segment.first_key;
          next_segments.push_back(left_seg);
          if (it->file_number != static_cast<uint64_t>(-1) && lifecycle_manager_) {
            lifecycle_manager_->Ref(it->file_number);
          }
        }
        // 2. 右侧剩余部分
        if (seg_end > new_end) {
          DataSegment right_seg = *it;
          right_seg.first_key = new_segment.last_key;
          next_segments.push_back(right_seg);
          if (it->file_number != static_cast<uint64_t>(-1) && lifecycle_manager_) {
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

    next_segments.push_back(new_segment);

    // 重新排序
    std::sort(next_segments.begin(), next_segments.end(),
              [](const DataSegment& a, const DataSegment& b) {
                return a.first_key < b.first_key;
              });

    snapshots = std::move(next_segments);

    // 插入这个新的 buffer snapshot，重新寻找插入位置进行合并相邻的内存段 (-1)
    auto insert_pos = std::find_if(snapshots.begin(), snapshots.end(),
                                   [&](const DataSegment& s) {
                                     return s.first_key == new_segment.first_key &&
                                            s.last_key == new_segment.last_key &&
                                            s.file_number == static_cast<uint64_t>(-1);
                                   });

    // 合并相邻的内存段 (-1)，因为之前切分+排序插入过了
    // 直接检查前后buffer segment，如果存在就合并成一个更大的 segment
    // MARK：多个 buffer segment 的情况？
    if (insert_pos != snapshots.end() && insert_pos->file_number == static_cast<uint64_t>(-1)) {
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


// void HotIndexTable::ReplaceOverlappingSegments(
//     uint64_t cuid, const DataSegment& new_segment,
//     const std::vector<uint64_t>& obsolete_delta_files) {
//   std::vector<DataSegment> segments_to_unref;

//   {
//     std::unique_lock<std::shared_mutex> lock(mutex_);
//     auto& entry = table_[cuid];
//     auto& snapshots = entry.snapshot_segments;

//     // 判定与new snapshot相关的snapshot -> !(End < New.Start || Start > New.End)
//     for (auto it = snapshots.begin(); it != snapshots.end();) {
//       bool is_left = (ExtractUserKey(it->last_key).ToString() <
//                       ExtractUserKey(new_segment.first_key).ToString());
//       bool is_right = (ExtractUserKey(it->first_key).ToString() >
//                        ExtractUserKey(new_segment.last_key).ToString());
//       std::string s1 = ExtractUserKey(new_segment.first_key).ToString();
//       std::string s2 = ExtractUserKey(new_segment.last_key).ToString();
//       std::string s3 = ExtractUserKey(it->first_key).ToString();
//       std::string s4 = ExtractUserKey(it->last_key).ToString();

//       if (!is_left && !is_right) {
//         segments_to_unref.push_back(*it);
//         it = snapshots.erase(it);
//       } else {
//         ++it;
//       }
//     }

//     // insert new_segment
//     auto it_insert_pos =
//         std::upper_bound(snapshots.begin(), snapshots.end(), new_segment,
//                          [](const DataSegment& a, const DataSegment& b) {
//                            return a.first_key < b.first_key;
//                          });
//     auto insert_pos = snapshots.insert(it_insert_pos, new_segment);

//     // 合并相邻的内存段 (-1)
//     if (new_segment.file_number == static_cast<uint64_t>(-1)) {
//       // 检查前一个
//       if (insert_pos != snapshots.begin()) {
//         auto prev = std::prev(insert_pos);
//         if (prev->file_number == static_cast<uint64_t>(-1)) {
//           insert_pos->first_key = prev->first_key;
//           snapshots.erase(prev);
//           // erase 后 insert_pos 依然指向插入的那个元素
//         }
//       }
//       // 检查后一个
//       auto next = std::next(insert_pos);
//       if (next != snapshots.end()) {
//         if (next->file_number == static_cast<uint64_t>(-1)) {
//           insert_pos->last_key = next->last_key;
//           snapshots.erase(next);
//         }
//       }
//     }

//     if (!obsolete_delta_files.empty()) {
//       auto& deltas = entry.deltas;

//       // 遍历 obsolete_delta_files
//       for (uint64_t file_num : obsolete_delta_files) {
//         // 在 entry.deltas 中找到并移除，同时移入 obsolete_deltas
//         for (auto it = deltas.begin(); it != deltas.end();) {
//           if (it->file_number == file_num) {
//             // 记录到 Obsolete (L0 Compaction再清除)
//             entry.obsolete_deltas.push_back(*it);
//             it = deltas.erase(it);
//             // 注意：一个 file_number 在 deltas 中可能出现多次吗？
//           } else {
//             ++it;
//           }
//         }
//       }
//     }
//   }  // Unlock

//   if (lifecycle_manager_) {
//     if (new_segment.file_number != static_cast<uint64_t>(-1)) {
//       lifecycle_manager_->Ref(new_segment.file_number);
//     }
//     for (const auto& seg : segments_to_unref) {
//       lifecycle_manager_->Unref(seg.file_number);
//     }
//   }
// }

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
    bool is_left = (ExtractUserKey(delta.last_key).ToString() <
                    ExtractUserKey(first_key).ToString());
    bool is_right = (ExtractUserKey(delta.first_key).ToString() >
                     ExtractUserKey(last_key).ToString());
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
    bool is_left = (ExtractUserKey(seg.last_key).ToString() <
                    ExtractUserKey(first_key).ToString());
    bool is_right = (ExtractUserKey(seg.first_key).ToString() >
                     ExtractUserKey(last_key).ToString());
    if (!is_left && !is_right) {
      overlapping_snapshots->push_back(seg);
    }
  }

  // 收集重叠的 delta segments
  for (const auto& seg : entry.deltas) {
    bool is_left = (ExtractUserKey(seg.last_key).ToString() <
                    ExtractUserKey(first_key).ToString());
    bool is_right = (ExtractUserKey(seg.first_key).ToString() >
                     ExtractUserKey(last_key).ToString());
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
  for (size_t i = 0; i < kNumShards; ++i) {
    std::shared_lock<std::shared_mutex> lock(shards_[i].mutex);
    total_cuids += shards_[i].table.size();
  }
  outfile << "Total Tracked CUIDs: " << total_cuids << std::endl;

  for (size_t i = 0; i < kNumShards; ++i) {
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