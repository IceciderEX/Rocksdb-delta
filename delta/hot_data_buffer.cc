#include "delta/hot_data_buffer.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

HotDataBuffer::HotDataBuffer(size_t threshold_bytes)
    : threshold_bytes_(threshold_bytes), total_size_bytes_(0) {
  buffer_.reserve(10000);
}

bool HotDataBuffer::Append(uint64_t cuid, const Slice& key, const Slice& value) {
  std::lock_guard<std::mutex> lock(mutex_);

  // shared buffer
  buffer_.push_back({cuid, key.ToString(), value.ToString()});
  
  size_t entry_size = key.size() + value.size();
  total_size_bytes_ += entry_size;

  // 检查 threshold
  return total_size_bytes_ >= threshold_bytes_;
}

std::vector<HotEntry> HotDataBuffer::ExtractAndReset() {
  std::lock_guard<std::mutex> lock(mutex_);

  std::vector<HotEntry> result;
  // 交换 ownership，clear buffer_
  result.swap(buffer_);
  
  // 重置全局计数
  total_size_bytes_ = 0;
  buffer_.reserve(10000);
  
  return result;
}

size_t HotDataBuffer::GetTotalSize() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return total_size_bytes_;
}

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