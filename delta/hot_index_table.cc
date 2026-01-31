#include "delta/hot_index_table.h"

namespace ROCKSDB_NAMESPACE {

void HotIndexTable::UpdateSnapshot(
    uint64_t cuid, const std::vector<DataSegment>& new_segments) {
  std::vector<DataSegment> segments_to_unref;
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto& entry = table_[cuid];
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
    std::unique_lock<std::shared_mutex> lock(mutex_);
    table_[cuid].snapshot_segments.push_back(segment);
  }

  if (lifecycle_manager_) {
    lifecycle_manager_->Ref(segment.file_number);
  }
}

bool HotIndexTable::PromoteSnapshot(uint64_t cuid,
                                    const DataSegment& new_segment) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it == table_.end()) {
    return false;
  }

  auto& entry = it->second;
  bool found_mem_segment = false;

  // 遍历 Snapshot 片段，寻找 file_number 为 -1 的片段，进行一个promote
  for (auto& seg : entry.snapshot_segments) {
    if (seg.file_number == static_cast<uint64_t>(-1)) {
      seg.file_number = new_segment.file_number;
      seg.first_key = new_segment.first_key;
      seg.last_key = new_segment.last_key;

      found_mem_segment = true;
      break;
    }
  }

  if (found_mem_segment && lifecycle_manager_) {
    lifecycle_manager_->Ref(new_segment.file_number);
    // -1 状态没有 Ref
  }

  return found_mem_segment;
}

void HotIndexTable::AddDelta(uint64_t cuid, const DataSegment& delta) {
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    table_[cuid].deltas.push_back(delta);
  }

  // if (lifecycle_manager_) {
  //   lifecycle_manager_->Ref(delta.file_number);
  // }
}

bool HotIndexTable::GetEntry(uint64_t cuid, HotIndexEntry* entry) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it != table_.end()) {
    if (entry) {
      *entry = it->second;
    }
    return true;
  }
  return false;
}

void HotIndexTable::MarkDeltasAsObsolete(uint64_t cuid) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it == table_.end()) return;

  auto& entry = it->second;
  if (entry.deltas.empty()) return;

  // 将 deltas 移动到 obsolete_deltas
  entry.obsolete_deltas.insert(entry.obsolete_deltas.end(),
                               entry.deltas.begin(), entry.deltas.end());
  entry.deltas.clear();
  // 等待L0Compaction清理它们
}

// d)
// 若遇到热点CUid，检查其热点索引表，若发现Deltas列表中对应的该段数据已被标记为
// Obsolete， 则直接跳过该段数据，并删除对应的Deltas记录。 Deprecated：不能在
// compaction 时直接清理
bool HotIndexTable::CheckAndRemoveObsoleteDeltas(
    uint64_t cuid, const std::vector<uint64_t>& input_files) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it == table_.end()) return false;

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
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it == table_.end()) return false;

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
  std::unique_lock<std::shared_mutex> lock(mutex_);

  std::unordered_set<uint64_t> input_files_set(input_files.begin(),
                                               input_files.end());

  for (uint64_t cuid : cuids) {
    auto entry_it = table_.find(cuid);
    if (entry_it == table_.end()) continue;

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
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto& entry = table_[cuid];

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
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = table_.find(cuid);
    if (it != table_.end()) {
      // Snapshot 片段
      if (it->second.HasSnapshot()) {
        segments_to_unref.insert(segments_to_unref.end(),
                                 it->second.snapshot_segments.begin(),
                                 it->second.snapshot_segments.end());
      }
      table_.erase(it);
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
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto& entry = table_[cuid];
    auto& snapshots = entry.snapshot_segments;

    // 判定与new snapshot相关的snapshot -> !(End < New.Start || Start > New.End)
    for (auto it = snapshots.begin(); it != snapshots.end();) {
      bool is_left = (it->last_key < new_segment.first_key);
      bool is_right = (it->first_key > new_segment.last_key);

      if (!is_left && !is_right) {
        segments_to_unref.push_back(*it);
        it = snapshots.erase(it);
      } else {
        ++it;
      }
    }

    // insert new_segment
    auto insert_pos =
        std::upper_bound(snapshots.begin(), snapshots.end(), new_segment,
                         [](const DataSegment& a, const DataSegment& b) {
                           return a.first_key < b.first_key;
                         });
    snapshots.insert(insert_pos, new_segment);

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
    lifecycle_manager_->Ref(new_segment.file_number);
    for (const auto& seg : segments_to_unref) {
      lifecycle_manager_->Unref(seg.file_number);
    }
  }
}

size_t HotIndexTable::CountOverlappingDeltas(
    uint64_t cuid, const std::string& first_key,
    const std::string& last_key) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it == table_.end()) return 0;

  const auto& entry = it->second;
  size_t count = 0;

  for (const auto& delta : entry.deltas) {
    // Key 范围重叠判断: !(delta.last < first || delta.first > last)
    bool is_left = (delta.last_key < first_key);
    bool is_right = (delta.first_key > last_key);
    if (!is_left && !is_right) {
      count++;
    }
  }
  return count;
}

void HotIndexTable::ClearAllForCuid(uint64_t cuid) {
  std::vector<DataSegment> segments_to_unref;
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = table_.find(cuid);
    if (it == table_.end()) return;

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
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it == table_.end()) return;

  const auto& entry = it->second;

  // 收集重叠的 snapshot segments
  for (const auto& seg : entry.snapshot_segments) {
    // !(seg.last < first || seg.first > last)
    bool is_left = (seg.last_key < first_key);
    bool is_right = (seg.first_key > last_key);
    if (!is_left && !is_right) {
      overlapping_snapshots->push_back(seg);
    }
  }

  // 收集重叠的 delta segments
  for (const auto& seg : entry.deltas) {
    bool is_left = (seg.last_key < first_key);
    bool is_right = (seg.first_key > last_key);
    if (!is_left && !is_right) {
      overlapping_deltas->push_back(seg);
    }
  }
}

void HotIndexTable::DumpToFile(const std::string& filename,
                               const std::string& phase_label) {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  // 使用追加模式打开文件
  std::ofstream outfile;
  outfile.open(filename, std::ios_base::app);

  if (!outfile.is_open()) {
    std::cerr << "Error: Unable to open dump file: " << filename << std::endl;
    return;
  }

  outfile << "========================================" << std::endl;
  outfile << "DUMP PHASE: " << phase_label << std::endl;
  outfile << "Total Tracked CUIDs: " << table_.size() << std::endl;

  for (const auto& kv : table_) {
    uint64_t cuid = kv.first;
    const HotIndexEntry& entry = kv.second;

    outfile << "CUID: " << cuid << std::endl;

    // 1. Snapshot 信息
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

    // 2. Deltas 信息
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

    // 3. Obsolete Deltas 信息
    if (!entry.obsolete_deltas.empty()) {
      outfile << "  [Obsolete]: " << entry.obsolete_deltas.size()
              << " pending cleanup" << std::endl;
      for (const auto& seg : entry.obsolete_deltas) {
        outfile << "    -> FileID: " << seg.file_number << " (Obs)"
                << std::endl;
      }
    }
  }
  outfile << "----------------------------------------" << std::endl
          << std::endl;
  outfile.close();
}

}  // namespace ROCKSDB_NAMESPACE