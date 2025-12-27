#include "delta/hot_index_table.h"

namespace ROCKSDB_NAMESPACE {

void HotIndexTable::UpdateSnapshot(uint64_t cuid, const std::string& path) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  // TODO：简单实现
  table_[cuid].snapshot_file_path = path;
}

bool HotIndexTable::GetEntry(uint64_t cuid, HotspotEntry* entry) const {
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

void HotIndexTable::RemoveEntry(uint64_t cuid) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  table_.erase(cuid);
}

}  // namespace ROCKSDB_NAMESPACE