#include "delta/hot_data_buffer.h"

namespace ROCKSDB_NAMESPACE {

HotDataBuffer::HotDataBuffer(size_t threshold_bytes)
    : threshold_bytes_(threshold_bytes), total_size_bytes_(0) {}

bool HotDataBuffer::Append(uint64_t cuid, const Slice& key, const Slice& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  HotBufferBatch& batch = buffers_[cuid];
  
  batch.keys.emplace_back(key.data(), key.size());
  batch.values.emplace_back(value.data(), value.size());
  
  size_t entry_size = key.size() + value.size();
  batch.size_bytes += entry_size;
  total_size_bytes_ += entry_size;

  // 检查 threshold
  return batch.size_bytes >= threshold_bytes_;
}

HotBufferBatch HotDataBuffer::ExtractBatch(uint64_t cuid) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  HotBufferBatch result_batch;
  auto it = buffers_.find(cuid);
  if (it != buffers_.end()) {
    result_batch = std::move(it->second);
    
    if (total_size_bytes_ >= result_batch.size_bytes) {
      total_size_bytes_ -= result_batch.size_bytes;
    } else {
      total_size_bytes_ = 0;
    }
    
    buffers_.erase(it);
  }
  return result_batch;
}

size_t HotDataBuffer::GetTotalSize() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return total_size_bytes_;
}

}  // namespace ROCKSDB_NAMESPACE