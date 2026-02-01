#pragma once

#include <deque>
#include <memory>
#include <mutex>
#include <string>

#include "delta/global_delete_count_table.h"
#include "delta/hot_data_buffer.h"
#include "delta/hot_index_table.h"
#include "delta/scan_frequency_table.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

struct ScanContext {
  uint64_t current_cuid = 0;
  // 当前 cuid 已访问的文件 ID (FileNumber...)
  std::unordered_set<uint64_t> visited_phys_units;
};

// Scan-as-Compaction 策略
enum class ScanAsCompactionStrategy {
  kNoAction,
  kFullReplace,  // full scan
  kPartialMerge  // 部分合并：读取相关 snapshots + deltas + scan 数据，归并更新
};

static constexpr size_t kPartialMergeDeltaThreshold =
    3;  // 触发部分合并的 delta 数量阈值

// 异步 Partial Merge 任务
struct PartialMergePendingTask {
  uint64_t cuid;
  std::string scan_first_key;
  std::string scan_last_key;
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
  // became_hot: 如果不为 nullptr，返回是否为首次成为热点
  bool RegisterScan(uint64_t cuid, bool is_full_scan,
                    bool* became_hot = nullptr);

  // 收集数据 (只有 RegisterScan 返回 true 时才调用此函数)
  bool BufferHotData(uint64_t cuid, const Slice& key, const Slice& value);

  Status FlushBlockToSharedSST(
      std::unique_ptr<HotDataBlock> block,
      std::unordered_map<uint64_t, DataSegment>* output_segments);

  void TriggerBufferFlush();

  bool ShouldTriggerScanAsCompaction(uint64_t cuid);

  // 根据扫描类型和涉及的 delta 数量，确定 Scan-as-Compaction 策略
  // is_full_scan: 是否为全版本扫描
  // scan_first_key/scan_last_key: 扫描的 key 范围（用于部分扫描的 delta 计算）
  ScanAsCompactionStrategy EvaluateScanAsCompactionStrategy(
      uint64_t cuid, bool is_full_scan, const std::string& scan_first_key = "",
      const std::string& scan_last_key = "",
      size_t* out_involved_delta_count = nullptr);

  // 计算给定 key 范围涉及的 delta 数量
  size_t CountInvolvedDeltas(uint64_t cuid, const std::string& first_key,
                             const std::string& last_key);

  void FinalizeScanAsCompaction(uint64_t cuid);

  // 带策略的 Finalize 方法，支持不同的策略处理
  void FinalizeScanAsCompactionWithStrategy(
      uint64_t cuid, ScanAsCompactionStrategy strategy,
      const std::string& scan_first_key = "",
      const std::string& scan_last_key = "");

  bool IsCuidDeleted(uint64_t cuid) { return delete_table_.IsDeleted(cuid); }

  bool IsHot(uint64_t cuid);

  // 拦截 Delete 操作?
  bool InterceptDelete(const Slice& key);

  uint64_t ExtractCUID(const Slice& key);

  // compaction 结束之后更新delta表
  void UpdateCompactionDelta(uint64_t cuid,
                             const std::vector<uint64_t>& input_files,
                             uint64_t output_file_number,
                             const std::string& first_key,
                             const std::string& last_key);

  // CompactionIterator 使用 否应该跳过
  bool ShouldSkipObsoleteDelta(uint64_t cuid,
                               const std::vector<uint64_t>& input_files);

  // CompactionJob cleanup：obsolete
  void CleanUpMetadataAfterCompaction(
      const std::unordered_set<uint64_t>& involved_cuids,
      const std::vector<uint64_t>& input_files);

  std::string GenerateSstFileName(uint64_t cuid);

  void UpdateCompactionRefCount(
      uint64_t cuid, int32_t input_count, int32_t output_count,
      const std::vector<uint64_t>& input_files_verified,
      uint64_t output_file_verified) {
    delete_table_.ApplyCompactionChange(cuid, input_count, output_count,
                                        input_files_verified,
                                        output_file_verified);
  }

  bool GetHotIndexEntry(uint64_t cuid, HotIndexEntry* out_entry) {
    return index_table_.GetEntry(cuid, out_entry);
  }

  InternalIterator* NewBufferIterator(uint64_t cuid,
                                      const InternalKeyComparator* icmp);

  // Getters for db_impl access
  const std::string& GetDataDir() const { return data_dir_; }
  std::shared_ptr<HotSstLifecycleManager> GetLifecycleManager() {
    return lifecycle_manager_;
  }

  HotIndexTable& GetIndexTable() { return index_table_; }

  GlobalDeleteCountTable& GetDeleteTable() { return delete_table_; }

  // 将 CUID 加入待初始化队列 (首次成为热点时调用)
  void EnqueueForInitScan(uint64_t cuid);

  // 获取并清空待初始化队列 (由 DBImpl 后台线程调用)
  std::vector<uint64_t> PopPendingInitCuids();

  // 检查是否有待处理的初始化任务
  bool HasPendingInitCuids() const;

  void DebugDump(const std::string& label) {
    std::string dump_file =
        "/home/wam/HWKV/rocksdb-delta/build/a_test_db/a_mgr.log";
    index_table_.DumpToFile(dump_file, label);
  }

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
  std::mutex buffered_cuids_mutex_;

  // 待初始化全量扫描的 CUID 队列
  std::vector<uint64_t> pending_init_cuids_;
  mutable std::mutex pending_init_mutex_;
  // Partial Merge 处理队列
  std::deque<PartialMergePendingTask> partial_merge_queue_;
  mutable std::mutex partial_merge_mutex_;

 public:
  // 加入 Partial Merge 队列 (前台调用，不阻塞)
  void EnqueuePartialMerge(uint64_t cuid, const std::string& scan_first_key,
                           const std::string& scan_last_key);

  // 处理 Partial Merge 任务 (后台调用)
  void ProcessPartialMerge();

  // 检查是否有待处理的 Partial Merge 任务
  bool HasPendingPartialMerge() const;

  // 取出一个待处理的 Partial Merge 任务 (供 db_impl 调用)
  bool PopPendingPartialMerge(PartialMergePendingTask* task);
};

}  // namespace ROCKSDB_NAMESPACE