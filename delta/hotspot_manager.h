#pragma once

#include <string>
#include <memory>
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "delta/hot_data_buffer.h"
#include "delta/hot_index_table.h"
#include "rocksdb/rocksdb_namespace.h"
#include "delta/scan_frequency_table.h"
#include "delta/global_delete_count_table.h"

namespace ROCKSDB_NAMESPACE {

struct ScanContext {
    uint64_t current_cuid = 0;
    // 当前 cuid 已访问的文件 ID (FileNumber...)
    std::unordered_set<uint64_t> visited_phys_units; 
};

class HotspotManager {
 public:
  // db_options: 用于初始化 SstFileWriter
  // data_dir: 生成 SST 文件存放的目录路径
  HotspotManager(const Options& db_options, const std::string& data_dir);

  ~HotspotManager() = default;

  // 返回数据给用户前调用 【暂时不要了】
  //   void OnUserScan(const Slice& key, const Slice& value);
  
  // 返回 true 表示该 CUID 是热点
  bool RegisterScan(uint64_t cuid);

  // 收集数据 (只有 RegisterScan 返回 true 时才调用此函数)
  bool BufferHotData(uint64_t cuid, const Slice& key, const Slice& value);

  Status FlushBlockToSharedSST(
    std::unique_ptr<HotDataBlock> block,
    std::unordered_map<uint64_t, DataSegment>* output_segments);

  void TriggerBufferFlush();

  HotIndexTable& GetIndexTable() { return index_table_; }

  GlobalDeleteCountTable& GetDeleteTable() { return delete_table_; }

  bool ShouldTriggerScanAsCompaction(uint64_t cuid);

  void FinalizeScanAsCompaction(uint64_t cuid);

  bool IsCuidDeleted(uint64_t cuid) {
      return delete_table_.IsDeleted(cuid);
  }

  bool IsHot(uint64_t cuid);

  // 拦截 Delete 操作?
  bool InterceptDelete(const Slice& key);

  uint64_t ExtractCUID(const Slice& key);

  void UpdateCompactionDelta(uint64_t cuid, 
                                           const std::vector<uint64_t>& input_files,
                                           uint64_t output_file_number,
                                           uint64_t offset,
                                           uint64_t length);
  
  // CompactionIterator 使用 否应该跳过
  bool ShouldSkipObsoleteDelta(uint64_t cuid, const std::vector<uint64_t>& input_files);

  // CompactionJob cleanup：obsolete
  void CleanUpMetadataAfterCompaction(const std::unordered_set<uint64_t>& involved_cuids,
                                      const std::vector<uint64_t>& input_files);

  std::string GenerateSstFileName(uint64_t cuid);

 private:
  Options db_options_;
  std::string data_dir_;

  std::shared_ptr<HotSstLifecycleManager> lifecycle_manager_;
  HotDataBuffer buffer_;
  HotIndexTable index_table_;

  ScanFrequencyTable frequency_table_;
  GlobalDeleteCountTable delete_table_;

  // 暂存正在进行的 Scan 所生成的 SST 片段
  // FinalizeScanAsCompaction -> IndexTable
  std::unordered_map<uint64_t, std::vector<DataSegment>> pending_snapshots_;
  std::mutex pending_mutex_;
  std::unordered_set<uint64_t> active_buffered_cuids_;
  std::mutex buffered_cuids_mutex_; // 保护上述集合
};

}  // namespace ROCKSDB_NAMESPACE