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
    // Old Deltas -> new Snapshot? 也需要 ref?
    if (!entry.deltas.empty()) {
      segments_to_unref.insert(segments_to_unref.end(), 
                               entry.deltas.begin(), 
                               entry.deltas.end());
      entry.deltas.clear(); 
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

void HotIndexTable::AppendSnapshotSegment(uint64_t cuid, const DataSegment& segment) {
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    table_[cuid].snapshot_segments.push_back(segment);
  }
  
  if (lifecycle_manager_) {
    lifecycle_manager_->Ref(segment.file_number);
  }
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