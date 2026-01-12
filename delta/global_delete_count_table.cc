// delta/global_delete_count_table.cc

#include "delta/global_delete_count_table.h"

namespace ROCKSDB_NAMESPACE {

bool GlobalDeleteCountTable::TrackPhysicalUnit(uint64_t cuid, uint64_t phys_id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto& entry = table_[cuid]; // Lazy Init
  
  if (entry.tracked_phys_ids.find(phys_id) == entry.tracked_phys_ids.end()) {
    entry.tracked_phys_ids.insert(phys_id);
    return true; // 新文件，Ref++
  }
  return false;
}

void GlobalDeleteCountTable::UntrackPhysicalUnit(uint64_t cuid, uint64_t phys_id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it != table_.end()) {
    it->second.tracked_phys_ids.erase(phys_id);
    // 如果计数归零且已标记删除的清理？
    // if (it->second.is_deleted && it->second.tracked_phys_ids.empty()) {
    //     table_.erase(it);
    // }
  }
}

// 用于 L0Compaction 对 delete cuid 的清理
void GlobalDeleteCountTable::UntrackFiles(uint64_t cuid, const std::vector<uint64_t>& file_ids) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it != table_.end()) {
    // 遍历本次 Compaction 的所有输入文件
    for (uint64_t fid : file_ids) {
      it->second.tracked_phys_ids.erase(fid);
    }
    // 检查是否归零且标记删除，如果是则清理条目
    if (it->second.tracked_phys_ids.empty() && it->second.is_deleted) {
      table_.erase(it);
    }
  }
}

bool GlobalDeleteCountTable::MarkDeleted(uint64_t cuid) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it != table_.end()) {
    it->second.is_deleted = true;
    return true; 
  }
  return false;
}

bool GlobalDeleteCountTable::IsDeleted(uint64_t cuid) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);  
  if (it != table_.end()) {
    return it->second.is_deleted;
  }
  return false;
}

int GlobalDeleteCountTable::GetRefCount(uint64_t cuid) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it != table_.end()) {
    return it->second.GetRefCount();
  }
  return 0;
}

bool GlobalDeleteCountTable::IsTracked(uint64_t cuid) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return table_.find(cuid) != table_.end();
}

} // namespace ROCKSDB_NAMESPACE