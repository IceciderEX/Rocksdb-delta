#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <optional>
#include "rocksdb/rocksdb_namespace.h"
#include "delta/hot_data_buffer.h"

namespace ROCKSDB_NAMESPACE {

struct DataSegment {
  uint64_t file_number;
  uint64_t offset;
  uint64_t length;
  std::string first_key;
  
  bool Valid() const { return length > 0; }
};

struct HotIndexEntry {
  std::vector<DataSegment> snapshot_segments;
  // std::vector<DataSegment> delta_segments; 

  std::vector<DataSegment> deltas;

  bool HasSnapshot() const { return !snapshot_segments.empty(); }
};

class HotIndexTable {
 public:
  explicit HotIndexTable(std::shared_ptr<HotSstLifecycleManager> lifecycle_manager)
      : lifecycle_manager_(lifecycle_manager) {}

  void UpdateSnapshot(uint64_t cuid, const std::vector<DataSegment>& new_segments);
  // scan-as-compaction -> add snapshots
  void AppendSnapshotSegment(uint64_t cuid, const DataSegment& segment);

  // 将内存中的 Snapshot (file_id = -1) 替换为真实 new_segment
  // 返回 true 表示成功找到并替换，false 表示未找到 -1 记录
  bool PromoteSnapshot(uint64_t cuid, const DataSegment& new_segment);

  void AddDelta(uint64_t cuid, const DataSegment& segment);

  bool GetEntry(uint64_t cuid, HotIndexEntry* entry) const;

  void RemoveCUID(uint64_t cuid);

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<uint64_t, HotIndexEntry> table_;
  std::shared_ptr<HotSstLifecycleManager> lifecycle_manager_;
};

}  // namespace ROCKSDB_NAMESPACE