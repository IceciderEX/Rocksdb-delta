// delta/scan_frequency_table.cc

#include "delta/scan_frequency_table.h"

namespace ROCKSDB_NAMESPACE {

ScanFrequencyTable::ScanFrequencyTable(int threshold, int window_sec)
    : threshold_(threshold), window_sec_(window_sec) {
  window_start_time_ = std::chrono::steady_clock::now();
}

bool ScanFrequencyTable::RecordAndCheckHot(uint64_t cuid) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  CheckAndRotateWindow();

  // 如果Scan某个CUid，添加计数，初始化为 0
  FrequencyEntry& entry = table_[cuid];

  // 如果该CUid已经被标记为热点CUid，认为不需要再统计频率
  if (entry.is_hot) {
    entry.count = 0; 
    return true;
  }

  // 每发生一次Scan，计数器加1
  entry.count++;

  // 当达到某个频率，判定为热点
  if (entry.count >= threshold_) {
    entry.is_hot = true;
    // is_hot 标记保留，直到被外部逻辑(如Compaction)清除或窗口重置
  }

  return entry.is_hot;
}

void ScanFrequencyTable::RemoveCUID(uint64_t cuid) {
  std::lock_guard<std::mutex> lock(mutex_);
  table_.erase(cuid);
}

void ScanFrequencyTable::CheckAndRotateWindow() {
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
      now - window_start_time_).count();

  // 窗口轮转时，重置
  if (elapsed >= window_sec_) {
    // 简单实现：清空整个表，所有 CUID 需要重新竞争上岗
    table_.clear();
    window_start_time_ = now;
  }
}

} // namespace ROCKSDB_NAMESPACE