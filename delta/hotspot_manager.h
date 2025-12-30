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

class HotspotManager {
 public:
  // db_options: 用于初始化 SstFileWriter
  // data_dir: 生成 SST 文件存放的目录路径
  HotspotManager(const Options& db_options, const std::string& data_dir);

  ~HotspotManager() = default;

  // 拦截接口：由 DBIterator 在准备返回数据给用户前调用
  void OnUserScan(const Slice& key, const Slice& value);

  Status FlushBufferToSST(uint64_t cuid);

  HotIndexTable& GetIndexTable() { return index_table_; }

  GlobalDeleteCountTable& GetDeleteTable() { return delete_table_; }

 private:
  uint64_t ExtractCUID(const Slice& key);

  std::string GenerateSstFileName(uint64_t cuid);

 private:
  Options db_options_;
  std::string data_dir_;

  HotDataBuffer buffer_;
  HotIndexTable index_table_;

  ScanFrequencyTable frequency_table_;
  GlobalDeleteCountTable delete_table_;
};

}  // namespace ROCKSDB_NAMESPACE