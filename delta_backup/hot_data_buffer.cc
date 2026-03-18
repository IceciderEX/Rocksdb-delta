#include "delta/hot_data_buffer.h"
#include <algorithm>
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

void HotDataBlock::Sort(const InternalKeyComparator* icmp) {
  for (auto& pair : buckets) {
     std::sort(pair.second.begin(), pair.second.end(), 
               [icmp](const HotEntry& a, const HotEntry& b) {
                 return icmp->Compare(a.key, b.key) < 0;
               });
  }
}

HotDataBuffer::HotDataBuffer(size_t threshold_bytes)
    : threshold_bytes_(threshold_bytes), total_buffered_size_(0) {
  for (size_t i = 0; i < kNumShards; ++i) {
    shards_[i].active_block = std::make_shared<HotDataBlock>();
  }
}

bool HotDataBuffer::Append(uint64_t cuid, const Slice& key,
                           const Slice& value) {
  Shard& shard = GetShard(cuid);
  size_t entry_size = key.size() + value.size();
  
  {
    std::lock_guard<std::mutex> lock(shard.mutex);
    shard.active_block->Add(cuid, key, value);
    shard.buffered_size += entry_size;
  }
  
  total_buffered_size_.fetch_add(entry_size);
  // 使用活跃计数器触发 Flush，确保只有积攒够 64MB 活跃数据才 Rotate
  size_t current_active = total_active_size_.fetch_add(entry_size) + entry_size;
  return current_active >= threshold_bytes_;
}

bool HotDataBuffer::RotateBuffer(const InternalKeyComparator* icmp) {
  // 合并所有分片的 active_block 为一个待刷盘的块
  auto combined_block = std::make_shared<HotDataBlock>();
  bool has_data = false;

  for (size_t i = 0; i < kNumShards; ++i) {
    std::shared_ptr<HotDataBlock> old_block;
    {
      std::lock_guard<std::mutex> lock(shards_[i].mutex);
      if (shards_[i].active_block->buckets.empty()) continue;
      
      old_block = shards_[i].active_block;
      shards_[i].active_block = std::make_shared<HotDataBlock>();
      shards_[i].buffered_size = 0;

      // 将旧块暂存到该分片的队列中，使其对 Reader 依然可见
      shards_[i].immutable_queue.push_back(old_block);
    }
    
    if (old_block) {
      has_data = true;
      size_t block_size = old_block->current_size_bytes;
      // 将分片数据合并到 combined
      for (auto& pair : old_block->buckets) {
        uint64_t cuid = pair.first;
        auto& bucket = pair.second;
        auto& target_bucket = combined_block->buckets[cuid];
        target_bucket.insert(target_bucket.end(), 
                             bucket.begin(), 
                             bucket.end());
        
        // 合并边界信息 (这里合并时先不考虑 icmp 顺序，后续由 Sort 最终校准)
        auto b_it = old_block->bounds.find(cuid);
        if (b_it != old_block->bounds.end()) {
          auto& target_bounds = combined_block->bounds[cuid];
          if (target_bounds.min_key.empty() || icmp->Compare(b_it->second.min_key, target_bounds.min_key) < 0) {
             target_bounds.min_key = b_it->second.min_key;
          }
          if (target_bounds.max_key.empty() || icmp->Compare(b_it->second.max_key, target_bounds.max_key) > 0) {
             target_bounds.max_key = b_it->second.max_key;
          }
        }
      }
      combined_block->current_size_bytes += block_size;
      // 从活跃计数器中减去即将进入不可变队列的大小
      total_active_size_.fetch_sub(block_size);
    }
  }

  if (has_data) {
    combined_block->Sort(icmp);
    
    std::lock_guard<std::mutex> lock(global_queue_mutex_);
    immutable_queue_.push_back(combined_block);

    // 数据已进入全局队列，可以清理各分片的暂存列表
    for (size_t i = 0; i < kNumShards; ++i) {
      std::lock_guard<std::mutex> shard_lock(shards_[i].mutex);
      shards_[i].immutable_queue.clear();
    }
    return true;
  }
  return false;
}

std::shared_ptr<HotDataBlock> HotDataBuffer::GetFrontBlockForFlush() {
  std::lock_guard<std::mutex> lock(global_queue_mutex_);
  if (immutable_queue_.empty()) return nullptr;
  return immutable_queue_.front();
}

void HotDataBuffer::PopFrontBlockAfterFlush() {
  std::shared_ptr<HotDataBlock> block;
  {
    std::lock_guard<std::mutex> lock(global_queue_mutex_);
    if (immutable_queue_.empty()) return;
    block = immutable_queue_.front();
    immutable_queue_.pop_front();
  }
  if (block) {
    total_buffered_size_.fetch_sub(block->current_size_bytes);
  }
}

bool HotDataBuffer::GetBoundaryKeys(uint64_t cuid, std::string* min_key,
                                    std::string* max_key,
                                    const InternalKeyComparator* icmp) {
  bool found = false;
  auto merge_bounds = [&](const HotDataBlock* block) {
    if (!block) return;
    auto it = block->bounds.find(cuid);
    if (it != block->bounds.end()) {
      if (!found) {
        *min_key = it->second.min_key;
        *max_key = it->second.max_key;
        found = true;
      } else {
        if (icmp->Compare(it->second.min_key, *min_key) < 0) *min_key = it->second.min_key;
        if (icmp->Compare(it->second.max_key, *max_key) > 0) *max_key = it->second.max_key;
      }
    }
  };

  // 1. 检查全局不可变队列
  {
    std::lock_guard<std::mutex> lock(global_queue_mutex_);
    for (const auto& block : immutable_queue_) {
      merge_bounds(block.get());
    }
  }

  // 2. 检查特定分片的活跃块及其暂存队列
  Shard& shard = GetShard(cuid);
  {
    std::lock_guard<std::mutex> lock(shard.mutex);
    for (const auto& block : shard.immutable_queue) {
      merge_bounds(block.get());
    }
    merge_bounds(shard.active_block.get());
  }

  return found;
}

// --------------------- HotDataBuffer Iterator --------------------- //

class HotDataBufferIterator : public InternalIterator {
 public:
  explicit HotDataBufferIterator(std::vector<HotEntry>&& entries,
                                 const InternalKeyComparator* icmp)
      : entries_(std::move(entries)), idx_(0), icmp_(icmp) {
  }

  bool Valid() const override { return idx_ < entries_.size(); }
  void SeekToFirst() override { idx_ = 0; }
  void SeekToLast() override {
    idx_ = entries_.empty() ? 0 : entries_.size() - 1;
  }

  void Seek(const Slice& target) override {
    auto it = std::lower_bound(entries_.begin(), entries_.end(), target,
                               [this](const HotEntry& entry, const Slice& val) {
                                 return icmp_->Compare(entry.key, val) < 0;
                               });
    idx_ = std::distance(entries_.begin(), it);
  }

  void SeekForPrev(const Slice& target) override {
    Seek(target);
    if (!Valid()) SeekToLast();
    while (Valid() && icmp_->Compare(entries_[idx_].key, target) > 0) {
      Prev();
    }
  }

  void Next() override { if (idx_ < entries_.size()) idx_++; }
  void Prev() override { if (idx_ > 0) idx_--; else idx_ = entries_.size(); }

  Slice key() const override { return entries_[idx_].key; }
  Slice value() const override { return entries_[idx_].value; }
  Status status() const override { return Status::OK(); }
  uint64_t GetPhysicalId() override { return 0; }

 private:
  std::vector<HotEntry> entries_;
  size_t idx_;
  const InternalKeyComparator* icmp_;
};

InternalIterator* HotDataBuffer::NewIterator(
    uint64_t cuid, const InternalKeyComparator* icmp) {
  
  std::vector<HotEntry> filtered_entries;

  // 1. Immutable
  {
    std::lock_guard<std::mutex> lock(global_queue_mutex_);
    for (const auto& block : immutable_queue_) {
      auto it = block->buckets.find(cuid);
      if (it != block->buckets.end()) {
        filtered_entries.insert(filtered_entries.end(), it->second.begin(), it->second.end());
      }
    }
  }

  // 2. active + rotating 中的暂存块
  Shard& shard = GetShard(cuid);
  {
    std::lock_guard<std::mutex> lock(shard.mutex);
    // Rotate 过程中的暂存块
    for (const auto& block : shard.immutable_queue) {
       auto it = block->buckets.find(cuid);
       if (it != block->buckets.end()) {
         filtered_entries.insert(filtered_entries.end(), it->second.begin(), it->second.end());
       }
    }
    auto it = shard.active_block->buckets.find(cuid);
    if (it != shard.active_block->buckets.end()) {
      filtered_entries.insert(filtered_entries.end(), it->second.begin(), it->second.end());
    }
  }

  // 虽然 bucket 内部有序，但不同 block 之间可能重叠？
  std::sort(filtered_entries.begin(), filtered_entries.end(),
            [icmp](const HotEntry& a, const HotEntry& b) {
              return icmp->Compare(Slice(a.key), Slice(b.key)) < 0;
            });

  return new HotDataBufferIterator(std::move(filtered_entries), icmp);
}


// SST Lifecycle Manager Implementation (remains same)
void HotSstLifecycleManager::RegisterFile(uint64_t file_number, const std::string& file_path, const std::string& link_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  files_[file_number] = {file_path, link_path, 0};
}
void HotSstLifecycleManager::Ref(uint64_t file_number) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = files_.find(file_number);
  if (it != files_.end()) it->second.ref_count++;
}
void HotSstLifecycleManager::Unref(uint64_t file_number) {
  std::string f_del, l_del;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = files_.find(file_number);
    if (it != files_.end() && --it->second.ref_count <= 0) {
      f_del = it->second.file_path; l_del = it->second.link_path;
      files_.erase(it);
    }
  }
  if (!l_del.empty()) env_->DeleteFile(l_del);
  if (!f_del.empty()) env_->DeleteFile(f_del);
}

}  // namespace ROCKSDB_NAMESPACE