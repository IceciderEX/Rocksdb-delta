// delta/global_delete_count_table.h

#pragma once
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <vector>
#include <unordered_set>
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct GDCTEntry {
  std::unordered_set<uint64_t> tracked_phys_ids;
  bool is_deleted = false;

  int GetRefCount() const { return static_cast<int>(tracked_phys_ids.size()); }
};

class GlobalDeleteCountTable {
 public:
  GlobalDeleteCountTable() = default;

  // 【Scan 阶段调用】
  // 增加引用计数 (当 Scan 发现一个新的 SST/Memtable 包含该 CUID 时调用)
  bool TrackPhysicalUnit(uint64_t cuid, uint64_t phys_id);

  void UntrackPhysicalUnit(uint64_t cuid, uint64_t phys_id);

  void UntrackFiles(uint64_t cuid, const std::vector<uint64_t>& file_ids);

  //  检查是否已经追踪了该 CUID
  bool IsTracked(uint64_t cuid) const;

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