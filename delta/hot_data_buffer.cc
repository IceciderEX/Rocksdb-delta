#include "delta/hot_data_buffer.h"

#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

void HotDataBlock::Sort() {
  // CUID 的数据连续
  std::sort(entries.begin(), entries.end(),
            [](const HotEntry& a, const HotEntry& b) {
              if (a.cuid != b.cuid) {
                return a.cuid < b.cuid;
              }
              return a.key < b.key;
            });
}

HotDataBuffer::HotDataBuffer(size_t threshold_bytes)
    : threshold_bytes_(threshold_bytes), total_buffered_size_(0) {
  active_block_ = std::make_unique<HotDataBlock>();
  active_block_->entries.reserve(10000);
}

bool HotDataBuffer::Append(uint64_t cuid, const Slice& key,
                           const Slice& value) {
  std::lock_guard<std::mutex> lock(mutex_);

  active_block_->Add(cuid, key, value);
  total_buffered_size_ += (key.size() + value.size());
  // threshold
  return active_block_->current_size_bytes >= threshold_bytes_;
}

bool HotDataBuffer::RotateBuffer() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (active_block_->entries.empty()) {
    return false;
  }

  immutable_queue_.push_back(std::move(active_block_));

  // new Active Block
  active_block_ = std::make_unique<HotDataBlock>();
  active_block_->entries.reserve(10000);
  return true;
}

std::unique_ptr<HotDataBlock> HotDataBuffer::ExtractBlockToFlush() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (immutable_queue_.empty()) {
    return nullptr;
  }

  auto block = std::move(immutable_queue_.front());
  immutable_queue_.pop_front();

  total_buffered_size_ -= block->current_size_bytes;

  return block;
}

// size_t HotDataBuffer::GetTotalSize() const {
//   std::lock_guard<std::mutex> lock(mutex_);
//   return total_size_bytes_;
// }

// --------------------- SST Lifecycle Management --------------------- //

void HotSstLifecycleManager::RegisterFile(uint64_t file_number,
                                          const std::string& file_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  files_[file_number] = {file_path, 0};
}

void HotSstLifecycleManager::Ref(uint64_t file_number) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = files_.find(file_number);
  if (it != files_.end()) {
    it->second.ref_count++;
  }
}

void HotSstLifecycleManager::Unref(uint64_t file_number) {
  std::string file_to_delete;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = files_.find(file_number);
    if (it != files_.end()) {
      it->second.ref_count--;
      if (it->second.ref_count <= 0) {
        // 引用计数为 0，物理删除文件
        file_to_delete = it->second.file_path;
        files_.erase(it);
      }
    }
  }

  if (!file_to_delete.empty()) {
    // 进行删除 sst 操作
    Status s = env_->DeleteFile(file_to_delete);
    if (!s.ok()) {
      // TODO: LOGGER
      fprintf(stderr, "[HotSstLifecycle] Failed to delete file %s: %s\n",
              file_to_delete.c_str(), s.ToString().c_str());
    } else {
      fprintf(stdout, "[HotSstLifecycle] Deleted obsolete file: %s\n",
              file_to_delete.c_str());
    }
  }
}

// --------------------- HotDataBuffer Iterator --------------------- //

class HotDataBufferIterator : public InternalIterator {
 public:
  explicit HotDataBufferIterator(std::vector<HotEntry>&& entries,
                                 const InternalKeyComparator* icmp)
      : entries_(std::move(entries)), idx_(0), icmp_(icmp) {
    if (entries_.empty()) {
      idx_ = 0;
    }
  }

  bool Valid() const override { return idx_ < entries_.size(); }

  void SeekToFirst() override { idx_ = 0; }

  void SeekToLast() override {
    if (entries_.empty()) {
      idx_ = 0;
    } else {
      idx_ = entries_.size() - 1;
    }
  }

  void Seek(const Slice& target) override {
    // seek to the first entry with key >= target
    auto it =
        std::lower_bound(entries_.begin(), entries_.end(), target,
                         [this](const HotEntry& entry, const Slice& val) {
                           if (this->icmp_) {
                             return this->icmp_->Compare(entry.key, val) < 0;
                           } else {
                             return entry.key < val.ToString();
                           }
                         });
    idx_ = std::distance(entries_.begin(), it);
  }

  void SeekForPrev(const Slice& target) override {
    Seek(target);
    if (!Valid()) {
      SeekToLast();
    }
    while (Valid()) {
      bool is_greater = false;
      if (icmp_) {
        is_greater = icmp_->Compare(entries_[idx_].key, target) > 0;
      } else {
        is_greater = entries_[idx_].key > target.ToString();
      }

      if (is_greater) {
        Prev();
      } else {
        break;
      }
    }
  }

  void Next() override {
    // idx++
    if (idx_ < entries_.size()) {
      idx_++;
    }
  }

  void Prev() override {
    if (idx_ > 0) {
      idx_--;
    } else {
      idx_ = entries_.size();
    }
  }

  Slice key() const override {
    assert(Valid());
    return entries_[idx_].key;
  }

  Slice value() const override {
    assert(Valid());
    return entries_[idx_].value;
  }

  Status status() const override { return Status::OK(); }

  uint64_t GetPhysicalId() override { return 0; }

 private:
  std::vector<HotEntry> entries_;
  size_t idx_;
  const InternalKeyComparator* icmp_;
};

bool HotDataBuffer::GetBoundaryKeys(uint64_t cuid, std::string* min_key,
                                    std::string* max_key) {
  std::lock_guard<std::mutex> lock(mutex_);
  bool found = false;

  auto merge_bounds_from_block = [&](const HotDataBlock* block) {
    if (!block) return;

    auto it = block->bounds.find(cuid);
    if (it != block->bounds.end()) {
      const std::string& blk_min = it->second.first;
      const std::string& blk_max = it->second.second;

      if (!found) {
        *min_key = blk_min;
        *max_key = blk_max;
        found = true;
      } else {
        if (blk_min < *min_key) *min_key = blk_min;
        if (blk_max > *max_key) *max_key = blk_max;
      }
    }
  };

  // 1. 检查 Immutable Queue
  for (const auto& block : immutable_queue_) {
    merge_bounds_from_block(block.get());
  }

  // 2. 检查 Active Block
  merge_bounds_from_block(active_block_.get());

  return found;
}

InternalIterator* HotDataBuffer::NewIterator(
    uint64_t cuid, const InternalKeyComparator* icmp) {
  std::lock_guard<std::mutex> lock(mutex_);

  std::vector<HotEntry> filtered_entries;

  // todo：这里的性能优化
  // Immutable Queue
  for (const auto& block : immutable_queue_) {
    // 遍历队列中的每个 Block
    for (const auto& entry : block->entries) {
      if (entry.cuid == cuid) {
        filtered_entries.push_back(entry);
      }
    }
  }

  // Active block 中查找
  if (active_block_) {
    for (const auto& entry : active_block_->entries) {
      if (entry.cuid == cuid) {
        filtered_entries.push_back(entry);
      }
    }
  }

  // sort
  if (icmp != nullptr) {
    std::sort(filtered_entries.begin(), filtered_entries.end(),
              [icmp](const HotEntry& a, const HotEntry& b) {
                return icmp->Compare(Slice(a.key), Slice(b.key)) < 0;
              });
  } else {
    fprintf(
        stderr,
        "[HotDataBuffer] NewIterator: icmp is nullptr, using fallback sort\n");
    // std::sort(filtered_entries.begin(), filtered_entries.end(),
    //     [](const HotEntry& a, const HotEntry& b) {
    //         return a.key < b.key;
    //     });
  }
  return new HotDataBufferIterator(std::move(filtered_entries), icmp);
}

}  // namespace ROCKSDB_NAMESPACE