// delta/global_delete_count_table.cc

#include "delta/global_delete_count_table.h"

namespace ROCKSDB_NAMESPACE {

void GlobalDeleteCountTable::IncrementRefCount(uint64_t cuid) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  table_[cuid].ref_count++;
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

void GlobalDeleteCountTable::DecrementRefCount(uint64_t cuid) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it != table_.end()) {
    it->second.ref_count--;
    
    if (it->second.ref_count <= 0) {
      table_.erase(it);
    }
  }
}

int GlobalDeleteCountTable::GetRefCount(uint64_t cuid) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it != table_.end()) {
    return it->second.ref_count;
  }
  return 0;
}

} // namespace ROCKSDB_NAMESPACE