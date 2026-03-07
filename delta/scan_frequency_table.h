// delta/scan_frequency_table.h

#pragma once
#include <unordered_map>
#include <mutex>
#include <chrono>
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct FrequencyEntry {
  int count = 0;     
  bool is_hot = false; 
};

class ScanFrequencyTable {
 public:
  ScanFrequencyTable(int threshold = 4, int window_sec = 600);

  // 返回值：当前是否为热点
  // became_hot：如果不为 nullptr，返回是否为首次成为热点
  bool RecordAndCheckHot(uint64_t cuid, bool* became_hot = nullptr);

  void RemoveCUID(uint64_t cuid);

  bool IsHot(uint64_t cuid) const;

 private:
  void CheckAndRotateWindow();

  int threshold_;
  int window_sec_;
  
  mutable std::mutex mutex_;
  std::unordered_map<uint64_t, FrequencyEntry> table_;
  
  std::chrono::steady_clock::time_point window_start_time_;
};

} // namespace ROCKSDB_NAMESPACE