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
};

struct HotspotEntry {
  std::vector<DataSegment> snapshot_segments;
  // std::vector<DataSegment> delta_segments; 

  bool IsValid() const {
    return !snapshot_segments.empty();
  }
};

struct HotIndexEntry {
  // 一个 CUID 的数据可能分散在多个共享 SST 中
  // 按写入时间顺序排列
  std::vector<DataSegment> segments;
};

class HotIndexTable {
 public:
  HotIndexTable() = default;

  // 添加一个新的数据片段 (Segment)
  void AddSegment(uint64_t cuid, const DataSegment& segment);

  // 获取某个 CUID 的所有数据位置
  bool GetEntry(uint64_t cuid, HotIndexEntry* entry) const;

  // 彻底移除某个 CUID (例如被 Tombstone 彻底清理)
  void RemoveCUID(uint64_t cuid);

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<uint64_t, HotspotEntry> table_;
  std::shared_ptr<HotSstLifecycleManager> lifecycle_manager_;
};

}  // namespace ROCKSDB_NAMESPACE