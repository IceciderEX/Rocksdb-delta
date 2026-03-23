#pragma once

#include <deque>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <array>
#include <atomic>

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

struct HotEntry {
  uint64_t cuid;
  std::string key;
  std::string value;

  // 用于排序
  bool operator<(const HotEntry& other) const { return key < other.key; }
};

// buffer block
struct HotDataBlock {
  // 按 CUID 分桶存储 entries，消除 NewIterator 时的线性过滤
  std::unordered_map<uint64_t, std::vector<HotEntry>> buckets;
  size_t current_size_bytes = 0;
  
  // 记录每个 CUID 的边界，用于快速判断
  struct Boundary {
     std::string min_key;
     std::string max_key;
  };
  std::unordered_map<uint64_t, Boundary> bounds;

  void Add(uint64_t c, const Slice& k, const Slice& v) {
    std::string k_str = k.ToString();
    std::string v_str = v.ToString();
    buckets[c].push_back({c, k_str, v_str});
    current_size_bytes += (k_str.size() + v_str.size());

    auto it = bounds.find(c);
    if (it == bounds.end()) {
      bounds[c] = {k_str, k_str};
    } else {
      if (k_str < it->second.min_key) it->second.min_key = k_str;
      if (k_str > it->second.max_key) it->second.max_key = k_str;
    }
  }

  void Sort(const InternalKeyComparator* icmp);  // 对每个 Bucket 内部按 Key 排序

  void Clear() {
    buckets.clear();
    current_size_bytes = 0;
    bounds.clear();
  }
};

class HotDataBuffer {
 public:
  explicit HotDataBuffer(size_t threshold_bytes = 1 * 1024 * 1024);

  // 将数据追加到对应 CUID 的 buffer 中
  // 如果 buffer 大小超过阈值，返回 true (need Flush)
  bool Append(uint64_t cuid, const Slice& key, const Slice& value);

  // Active Buffer -> Immutable Queue
  bool RotateBuffer(const InternalKeyComparator* icmp);
  std::shared_ptr<HotDataBlock> GetFrontBlockForFlush();
  void PopFrontBlockAfterFlush();

  size_t GetTotalBufferedSize() const { return total_buffered_size_; }

  bool GetBoundaryKeys(uint64_t cuid, std::string* min_key,
                       std::string* max_key,
                       const InternalKeyComparator* icmp);

  // for reading
  InternalIterator* NewIterator(uint64_t cuid,
                                const InternalKeyComparator* icmp);

 private:
  static constexpr size_t kNumShards = 32;
  
  // 将 Append 操作分片，减少并发写入冲突
  struct Shard {
    mutable std::mutex mutex;
    std::shared_ptr<HotDataBlock> active_block;
    std::deque<std::shared_ptr<HotDataBlock>> immutable_queue;
    std::atomic<size_t> buffered_size{0};
  };

  Shard& GetShard(uint64_t cuid) {
    return shards_[cuid % kNumShards];
  }

  size_t threshold_bytes_;
  std::atomic<size_t> total_buffered_size_{0};
  std::atomic<size_t> total_active_size_{0};  // 仅统计尚未 Rotate 的活跃数据
  
  mutable std::mutex global_queue_mutex_;
  std::deque<std::shared_ptr<HotDataBlock>> immutable_queue_;

  std::array<Shard, kNumShards> shards_;
};

class HotSstLifecycleManager {
 public:
  HotSstLifecycleManager(const Options& options) : 
    env_(options.env), info_log_(options.info_log) {}

  void RegisterFile(uint64_t file_number, const std::string& file_path,
                    const std::string& link_path);
  void Ref(uint64_t file_number);
  void Unref(uint64_t file_number);

 private:
  Env* env_;
  std::shared_ptr<Logger> info_log_;
  std::mutex mutex_;

  struct FileState {
    std::string file_path;
    std::string link_path;
    int ref_count;
  };

  std::unordered_map<uint64_t, FileState> files_;
};

}  // namespace ROCKSDB_NAMESPACE