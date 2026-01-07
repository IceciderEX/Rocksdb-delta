#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <mutex>
#include <deque>
#include "rocksdb/slice.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

struct HotEntry {
  uint64_t cuid;
  std::string key;
  std::string value;
  // uint64_t seq;
  
  // 用于排序
  bool operator<(const HotEntry& other) const {
    return key < other.key; 
  }
};

// buffer block
struct HotDataBlock {
  std::vector<HotEntry> entries;
  size_t current_size_bytes = 0;
  
  void Add(uint64_t c, const Slice& k, const Slice& v) {
    entries.push_back({c, k.ToString(), v.ToString()});
    current_size_bytes += (k.size() + v.size());
  }
  
  void Sort(); // 按 CUID, Key 排序
};


class HotDataBuffer {
 public:
  // explicit HotDataBuffer(size_t threshold_bytes = 64 * 1024 * 1024); // 默认 64MB
  explicit HotDataBuffer(size_t threshold_bytes = 1 * 1024 * 1024); // 先用 1MB 测试

  // 将数据追加到对应 CUID 的 buffer 中
  // 如果 buffer 大小超过阈值，返回 true (need Flush)
  bool Append(uint64_t cuid, const Slice& key, const Slice& value);

  // Active Buffer -> Immutable Queue
  bool RotateBuffer();

  std::unique_ptr<HotDataBlock> ExtractBlockToFlush();

  // std::vector<HotEntry> ExtractAndReset();

  size_t GetTotalBufferedSize() const { return total_buffered_size_; }

 private:
  // flush threshold
  size_t threshold_bytes_;
  std::atomic<size_t> total_buffered_size_;
  mutable std::mutex mutex_;

  std::unique_ptr<HotDataBlock> active_block_;
  std::deque<std::unique_ptr<HotDataBlock>> immutable_queue_;
};

class HotSstLifecycleManager {
 public:
  HotSstLifecycleManager(const Options& options) : env_(options.env) {}

  // 注册一个新生成的文件，初始引用计数为 0
  void RegisterFile(uint64_t file_number, const std::string& file_path);

  // 增加引用计数 (当 IndexTable 添加指向该文件的 Segment 时调用)
  void Ref(uint64_t file_number);

  // 减少引用计数 (当 IndexTable 删除 Segment 或 Compaction 完成后调用)
  // 如果计数归零，物理删除文件
  void Unref(uint64_t file_number);

 private:
  Env* env_;
  std::mutex mutex_;
  
  struct FileState {
    std::string file_path;
    int ref_count;
  };
  
  std::unordered_map<uint64_t, FileState> files_;
};

}  // namespace ROCKSDB_NAMESPACE