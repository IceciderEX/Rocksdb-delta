#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <mutex>
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

// 一个 CUID 对应的数据
struct HotBufferBatch {
  std::vector<std::string> keys;
  std::vector<std::string> values;
  size_t size_bytes = 0;

  void Clear() {
    keys.clear();
    values.clear();
    size_bytes = 0;
  }

  bool IsEmpty() const {
    return keys.empty();
  }
};

class HotDataBuffer {
 public:
  explicit HotDataBuffer(size_t threshold_bytes = 64 * 1024 * 1024); // 默认 64MB

  // 将数据追加到对应 CUID 的 buffer 中
  // 如果 buffer 大小超过阈值，返回 true (need Flush)
  bool Append(uint64_t cuid, const Slice& key, const Slice& value);

  // 获取并清空指定 CUID 的数据 
  HotBufferBatch ExtractBatch(uint64_t cuid);

  size_t GetTotalSize() const;

 private:
  // flush threshold
  size_t threshold_bytes_;
  mutable std::mutex mutex_;
  // CUID -> BufferBatch 
  std::unordered_map<uint64_t, HotBufferBatch> buffers_;
  // 全局总大小统计
  size_t total_size_bytes_;
};

}  // namespace ROCKSDB_NAMESPACE