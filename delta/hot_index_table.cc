#include "delta/hot_index_table.h"

namespace ROCKSDB_NAMESPACE {

void HotIndexTable::UpdateSnapshot(uint64_t cuid, const std::vector<DataSegment>& new_segments) {
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
    
    // TODO(delta)：进一步确认
    // // Old Deltas -> new Snapshot? 也需要 ref?
    // if (!entry.deltas.empty()) {
    //   segments_to_unref.insert(segments_to_unref.end(), 
    //                            entry.deltas.begin(), 
    //                            entry.deltas.end());
    //   entry.deltas.clear(); 
    // }

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

void HotIndexTable::AppendSnapshotSegment(uint64_t cuid, const DataSegment& segment) {
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    table_[cuid].snapshot_segments.push_back(segment);
  }
  
  if (lifecycle_manager_) {
    lifecycle_manager_->Ref(segment.file_number);
  }
}

bool HotIndexTable::PromoteSnapshot(uint64_t cuid, const DataSegment& new_segment) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it == table_.end()) {
    return false;
  }

  auto& entry = it->second;
  bool found_mem_segment = false;

  // 遍历 Snapshot 片段，寻找 file_number 为 -1 的片段
  for (auto& seg : entry.snapshot_segments) {
    if (seg.file_number == static_cast<uint64_t>(-1)) {
      // 找到了内存片段，原地更新为真实文件信息 (Offset/Length/FileNum)
      seg = new_segment; 
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
  
  if (lifecycle_manager_) {
    lifecycle_manager_->Ref(delta.file_number);
  }
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
                               entry.deltas.begin(), 
                               entry.deltas.end());
  entry.deltas.clear();
  // 等待L0Compaction清理它们
}

// d)	若遇到热点CUid，检查其热点索引表，若发现Deltas列表中对应的该段数据已被标记为 Obsolete，
// 则直接跳过该段数据，并删除对应的Deltas记录。
bool HotIndexTable::CheckAndRemoveObsoleteDeltas(uint64_t cuid, const std::vector<uint64_t>& input_files) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it == table_.end()) return false;

  auto& entry = it->second;
  if (entry.obsolete_deltas.empty()) return false;

  bool found_obsolete = false;
  
  for (auto d_it = entry.obsolete_deltas.begin(); d_it != entry.obsolete_deltas.end(); ) {
    bool is_input_file = false;
    for (uint64_t fid : input_files) {
        if (d_it->file_number == fid) {
            is_input_file = true;
            break;
        }
    }
    if (is_input_file) {
        found_obsolete = true;    
        // 减少 SST 引用计数
        if (lifecycle_manager_) {
            lifecycle_manager_->Unref(d_it->file_number);
        }
        d_it = entry.obsolete_deltas.erase(d_it);
    } else {
        ++d_it;
    }
  }
  return found_obsolete;
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
      // Delta 片段
      segments_to_unref.insert(segments_to_unref.end(), 
                               it->second.deltas.begin(), 
                               it->second.deltas.end());
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

}  // namespace ROCKSDB_NAMESPACE