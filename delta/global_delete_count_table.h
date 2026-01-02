// delta/global_delete_count_table.h

#pragma once
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct GDCTEntry {
  int ref_count = 0;    
  bool is_deleted = false; 
};

class GlobalDeleteCountTable {
 public:
  GlobalDeleteCountTable() = default;

  // 【Scan 阶段调用】
  // 增加引用计数 (当 Scan 发现一个新的 SST/Memtable 包含该 CUID 时调用)
  void IncrementRefCount(uint64_t cuid);

  //  检查是否已经追踪了该 CUID
  bool IsTracked(uint64_t cuid) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return table_.find(cuid) != table_.end();
  }

  void InitRefCount(uint64_t cuid, int count = 1) {
      std::unique_lock<std::shared_mutex> lock(mutex_);
      table_[cuid].ref_count += count;
  }

  bool TryRegister(uint64_t cuid) {
      std::unique_lock<std::shared_mutex> lock(mutex_);
      if (table_.find(cuid) == table_.end()) {
          table_[cuid] = {0, false}; 
          return true;
      }
      return false;
  }

  // 【Delete 阶段调用】
  // 直接在表中标记为 True，避免写 Tombstone
  bool MarkDeleted(uint64_t cuid);

  // 【Compaction/Read 阶段调用】
  // 检查是否已删除 (用于过滤数据)
  bool IsDeleted(uint64_t cuid) const;

  // 【Compaction 阶段调用】
  // 物理清理后减少引用计数
  void DecrementRefCount(uint64_t cuid);

  int GetRefCount(uint64_t cuid) const;

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<uint64_t, GDCTEntry> table_;
};

} // namespace ROCKSDB_NAMESPACE