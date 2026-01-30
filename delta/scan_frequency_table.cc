// delta/scan_frequency_table.cc

#include "delta/scan_frequency_table.h"

namespace ROCKSDB_NAMESPACE {

ScanFrequencyTable::ScanFrequencyTable(int threshold, int window_sec)
    : threshold_(threshold), window_sec_(window_sec) {
  window_start_time_ = std::chrono::steady_clock::now();
}

void ScanFrequencyTable::CheckAndRotateWindow() {
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
      now - window_start_time_).count();

  // 窗口轮转时，重置
  if (elapsed >= window_sec_) {
    // 先进行一个简单的实现：直接清空整个表
    table_.clear();
    window_start_time_ = now;
  }
}

bool ScanFrequencyTable::IsHot(uint64_t cuid) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it != table_.end()) {
    return it->second.is_hot;
  }
  return false;
}

bool ScanFrequencyTable::RecordAndCheckHot(uint64_t cuid, bool* became_hot) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // 初始化输出参数
  if (became_hot) {
    *became_hot = false;
  }
  
  CheckAndRotateWindow();

  FrequencyEntry& entry = table_[cuid];

  // 如果该CUid已经被标记为热点CUid，认为不需要再统计频率
  if (entry.is_hot) {
    entry.count = 0; 
    return true;
  }

  // 每发生一次Scan，计数器加1
  entry.count++;
  if (entry.count >= threshold_) {
    entry.is_hot = true;
    // 首次成为热点
    if (became_hot) {
      *became_hot = true;
    }
  }

  return entry.is_hot;
}

void ScanFrequencyTable::RemoveCUID(uint64_t cuid) {
  std::lock_guard<std::mutex> lock(mutex_);
  table_.erase(cuid);
}
} // namespace ROCKSDB_NAMESPACE