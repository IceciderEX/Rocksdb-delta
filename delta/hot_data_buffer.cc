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

bool HotDataBuffer::Append(uint64_t cuid, const Slice& key, const Slice& value) {
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

void HotSstLifecycleManager::RegisterFile(uint64_t file_number, const std::string& file_path) {
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
      fprintf(stdout, "[HotSstLifecycle] Deleted obsolete file: %s\n", file_to_delete.c_str());
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE