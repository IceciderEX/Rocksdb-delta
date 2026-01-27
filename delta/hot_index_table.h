#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <optional>
#include <fstream>
#include <iostream>
#include "rocksdb/rocksdb_namespace.h"
#include "delta/hot_data_buffer.h"

namespace ROCKSDB_NAMESPACE {

struct DataSegment {
  uint64_t file_number;
  std::string first_key;
  std::string last_key;
  
  bool Valid() const { return !first_key.empty(); }
};

struct HotIndexEntry {
  std::vector<DataSegment> snapshot_segments;
  // std::vector<DataSegment> delta_segments; 

  std::vector<DataSegment> deltas;
  // Scan-as-Compaction 标记
  std::vector<DataSegment> obsolete_deltas;

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

  void MarkDeltasAsObsolete(uint64_t cuid);

  bool CheckAndRemoveObsoleteDeltas(uint64_t cuid, const std::vector<uint64_t>& input_files);

  bool GetEntry(uint64_t cuid, HotIndexEntry* entry) const;

  void RemoveCUID(uint64_t cuid);

  void UpdateDeltaIndex(uint64_t cuid, 
                                     const std::vector<uint64_t>& input_files,
                                     const DataSegment& new_delta);
  
  // 用于 CompactionIterator 决定是否 Skip
  bool IsDeltaObsolete(uint64_t cuid, const std::vector<uint64_t>& input_files) const;

  // 用于清理cuid的 Obsolete Deltas
  void RemoveObsoleteDeltasForCUIDs(const std::unordered_set<uint64_t>& cuids, 
                                    const std::vector<uint64_t>& input_files);    
                                    
  void DumpToFile(const std::string& filename, const std::string& phase_label);

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<uint64_t, HotIndexEntry> table_;
  std::shared_ptr<HotSstLifecycleManager> lifecycle_manager_;
};

}  // namespace ROCKSDB_NAMESPACE