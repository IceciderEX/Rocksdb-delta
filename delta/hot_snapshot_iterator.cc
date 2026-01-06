#include "delta/hot_snapshot_iterator.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

HotSnapshotIterator::HotSnapshotIterator(
    uint64_t cuid, 
    const std::vector<DataSegment>& segments,
    std::shared_ptr<HotSstLifecycleManager> lifecycle_manager,
    const Options& options)
    : cuid_(cuid),
      segments_(segments),
      lifecycle_manager_(lifecycle_manager),
      options_(options),
      current_segment_index_(0),
      valid_(false) {
}

void HotSnapshotIterator::SeekToFirst() {
  if (segments_.empty()) {
    valid_ = false;
    return;
  }
  
  // 从第 0 个片段开始
  LoadSegment(0);
  
  // 定位到该 Segment 中属于 CUID 的起始位置
  if (current_file_iter_) {
    std::string seek_key = MakeSeekKey(cuid_);
    current_file_iter_->Seek(seek_key);
    
    // 检查
    AdvanceToNextValidSegment();
  }
}

void HotSnapshotIterator::Seek(const Slice& target) {
    // 目前简单的实现：Reset 到 0，然后找
    // segments 的 key 范围索引来 jump？
    if (segments_.empty()) {
        valid_ = false;
        return;
    }

    LoadSegment(0); 
    if (current_file_iter_) {
        current_file_iter_->Seek(target);
        AdvanceToNextValidSegment();
    }
}

void HotSnapshotIterator::Next() {
  if (!valid_) return;

  assert(current_file_iter_);
  current_file_iter_->Next();
  
  AdvanceToNextValidSegment();
}

void HotSnapshotIterator::AdvanceToNextValidSegment() {
  while (true) {
    if (current_file_iter_ && current_file_iter_->Valid()) {
      if (ExtractCUID(current_file_iter_->key()) == cuid_) {
        valid_ = true;
        return; 
      }
    }

    // 当前 Segment 没了，尝试加载下一个
    current_segment_index_++;
    if (current_segment_index_ >= segments_.size()) {
      valid_ = false;
      current_file_reader_.reset();
      current_file_iter_.reset();
      return;
    }

    LoadSegment(current_segment_index_);
    
    // Seek 到 CUID 的起始位置
    if (current_file_iter_) {
        std::string seek_key = MakeSeekKey(cuid_);
        current_file_iter_->Seek(seek_key);
    }
  }
}

void HotSnapshotIterator::LoadSegment(size_t index) {
  current_segment_index_ = index;
  if (index >= segments_.size()) {
    valid_ = false;
    return;
  }

  const auto& seg = segments_[index];
  std::string file_path = lifecycle_manager_->GetFilePath(seg.file_number);
  
  if (file_path.empty()) {
    status_ = Status::Corruption("Hot SST file not found in lifecycle manager");
    valid_ = false;
    return;
  }

  current_file_reader_.reset(new SstFileReader(options_));
  Status s = current_file_reader_->Open(file_path);
  if (!s.ok()) {
    status_ = s;
    valid_ = false;
    current_file_reader_.reset();
    return;
  }

  ReadOptions ro; // TODO: 传递 ReadOptions
  current_file_iter_.reset(current_file_reader_->NewIterator(ro));
}

// 辅助函数：构造 CUID 的最小 Key
std::string HotSnapshotIterator::MakeSeekKey(uint64_t cuid) {
  // TODO(User): 请在 DataSegment 中添加 `std::string start_key` 并在 Flush 时记录。
  return ""; 
}

uint64_t HotSnapshotIterator::ExtractCUID(const Slice& key) const {
  if (key.size() < 24) return 0;
  const unsigned char* p = reinterpret_cast<const unsigned char*>(key.data()) + 16;
  uint64_t c = (static_cast<uint64_t>(p[0]) << 56) |
               (static_cast<uint64_t>(p[1]) << 48) |
               (static_cast<uint64_t>(p[2]) << 40) |
               (static_cast<uint64_t>(p[3]) << 32) |
               (static_cast<uint64_t>(p[4]) << 24) |
               (static_cast<uint64_t>(p[5]) << 16) |
               (static_cast<uint64_t>(p[6]) << 8)  |
               (static_cast<uint64_t>(p[7]));
  return c;
}

bool HotSnapshotIterator::Valid() const { return valid_; }
Slice HotSnapshotIterator::key() const { return current_file_iter_->key(); }
Slice HotSnapshotIterator::value() const { return current_file_iter_->value(); }
Status HotSnapshotIterator::status() const { return status_; }

void HotSnapshotIterator::SeekToLast() { /* TODO: 较少用到 */ valid_ = false; }
void HotSnapshotIterator::SeekForPrev(const Slice& target) { /* TODO */ valid_ = false; }
void HotSnapshotIterator::Prev() { /* TODO */ valid_ = false; }

}  // namespace ROCKSDB_NAMESPACE