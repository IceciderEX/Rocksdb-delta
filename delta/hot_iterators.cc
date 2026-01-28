#include "delta/hot_iterators.h"
#include "table/merging_iterator.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

static FileMetaData MakeFileMetaFromSegment(const DataSegment& seg) {
    FileMetaData meta;
    // file_number, path_id=0, file_size=0 (unknown/cached)
    meta.fd = FileDescriptor(seg.file_number, 0, 0); 
    // 这里需要是internal key
    meta.smallest.DecodeFrom(seg.first_key);
    meta.largest.DecodeFrom(seg.last_key);
    return meta;
}

// ======================= HotDeltaIterator ==========================

HotDeltaIterator::HotDeltaIterator(const std::vector<DataSegment>& deltas,
                                   TableCache* table_cache,
                                   const ReadOptions& read_options,
                                   const FileOptions& file_options,
                                   const InternalKeyComparator& icmp,
                                   const MutableCFOptions& mutable_cf_options,
                                   bool allow_unprepared_value)
    : deltas_(deltas) { // Copy deltas
  
  std::vector<InternalIterator*> children;
  children.reserve(deltas_.size());
  bounds_slices_.reserve(deltas_.size() * 2);

  for (const auto& delta : deltas_) {
    // Bounds
    bounds_slices_.emplace_back(delta.first_key);
    const Slice* lower_ptr = &bounds_slices_.back();
    bounds_slices_.emplace_back(delta.last_key);
    const Slice* upper_ptr = &bounds_slices_.back();
    // ReadOptions
    ReadOptions ro = read_options;
    ro.iterate_lower_bound = lower_ptr;
    ro.iterate_upper_bound = upper_ptr;

    FileDescriptor fd(delta.file_number, 0, 0); // PathId=0, Size=0(unknown)
    FileMetaData meta;
    meta.fd = fd;
    
    InternalIterator* iter = table_cache->NewIterator(
        ro, file_options, icmp, // InternalKeyComparator
        meta, // FileMetaData
        nullptr, mutable_cf_options, nullptr, nullptr,
        TableReaderCaller::kUserIterator, nullptr, false, 
        0, // L0
        0, nullptr, nullptr, allow_unprepared_value, nullptr, nullptr               
    );

    if (iter) {
      children.push_back(iter);
    }
  }

  merging_iter_ = NewMergingIterator(&icmp, children.data(), 
                                     static_cast<int>(children.size()));
}

HotDeltaIterator::~HotDeltaIterator() {
  if (merging_iter_) {
    delete merging_iter_;
  }
}

// agent
bool HotDeltaIterator::Valid() const { return merging_iter_->Valid(); }
void HotDeltaIterator::SeekToFirst() { merging_iter_->SeekToFirst(); }
void HotDeltaIterator::SeekToLast() { merging_iter_->SeekToLast(); }
void HotDeltaIterator::Seek(const Slice& target) { merging_iter_->Seek(target); }
void HotDeltaIterator::SeekForPrev(const Slice& target) { merging_iter_->SeekForPrev(target); }
void HotDeltaIterator::Next() { merging_iter_->Next(); }
void HotDeltaIterator::Prev() { merging_iter_->Prev(); }
Slice HotDeltaIterator::key() const { return merging_iter_->key(); }
Slice HotDeltaIterator::value() const { return merging_iter_->value(); }
Status HotDeltaIterator::status() const { return merging_iter_->status(); }


// ====================== HotSnapshotIterator ============================

HotSnapshotIterator::HotSnapshotIterator(const std::vector<DataSegment>& segments,
                                         HotspotManager* hotspot_manager,
                                         TableCache* table_cache,
                                         const ReadOptions& read_options,
                                         const FileOptions& file_options,
                                         const InternalKeyComparator& icmp)
    : segments_(segments),
      hotspot_manager_(hotspot_manager),
      table_cache_(table_cache),
      read_options_(read_options),
      file_options_(file_options),
      icmp_(icmp),
      current_segment_index_(-1),
      status_(Status::OK()) {
}

HotSnapshotIterator::~HotSnapshotIterator() {
  // current_iter_ unique_ptr auto released
}

void HotSnapshotIterator::InitIterForSegment(size_t index) {
  if (index >= segments_.size()) {
    current_iter_.reset(nullptr);
    current_segment_index_ = -1;
    return;
  }

  const auto& seg = segments_[index];
  current_segment_index_ = static_cast<int>(index);

  if (seg.file_number == static_cast<uint64_t>(-1)) {
    // Case A: 内存 Buffer fileid -1
    InternalIterator* mem_iter = hotspot_manager_->NewBufferIterator(); 
    current_iter_.reset(mem_iter);
  } else {
    // Case B: 物理 SST
    FileDescriptor fd(seg.file_number, 0, 0);
    FileMetaData meta;
    meta.fd = fd;
    
    ReadOptions ro = read_options_;
    current_lower_bound_ = Slice(seg.first_key);
    current_upper_bound_ = Slice(seg.last_key);
    ro.iterate_lower_bound = &current_lower_bound_; 
    ro.iterate_upper_bound = &current_upper_bound_;

    InternalIterator* iter = table_cache_->NewIterator(
        ro,
        file_options_,
        icmp_,
        meta,
        nullptr,
        mutable_cf_options_,
        nullptr,
        nullptr,
        TableReaderCaller::kUserIterator,
        nullptr,
        false,
        1, // L1+
        0,
        nullptr,
        nullptr,
        false, // allow_unprepared_value
        nullptr,
        nullptr
    );
    
    current_iter_.reset(iter);
  }
}

void HotSnapshotIterator::Seek(const Slice& target) {
  if (segments_.empty()) {
    current_iter_.reset(nullptr);
    return;
  }

  // EndKey >= Target 的 Segment
  auto it = std::lower_bound(segments_.begin(), segments_.end(), target,
      [&](const DataSegment& seg, const Slice& val) {
        // 比较 seg.last_key < val
        return icmp_.user_comparator()->Compare(ExtractUserKey(seg.last_key), ExtractUserKey(val)) < 0;
      });

  size_t index = std::distance(segments_.begin(), it);
  
  if (it == segments_.end()) {
    current_iter_.reset(nullptr);
    current_segment_index_ = -1;
    return;
  }

  if (static_cast<int>(index) != current_segment_index_) {
    InitIterForSegment(index);
  }

  // segment seek
  if (current_iter_) {
    current_iter_->Seek(target);
    
    if (!current_iter_->Valid()) {
      SwitchToNextSegment();
    }
  }
}

void HotSnapshotIterator::Next() {
  if (!current_iter_) return;
  
  current_iter_->Next();
  
  if (!current_iter_->Valid()) {
    // 当前 Segment 耗尽，切换到下一个
    SwitchToNextSegment();
    if (current_iter_) {
      current_iter_->SeekToFirst();
    }
  }
}

void HotSnapshotIterator::SwitchToNextSegment() {
  InitIterForSegment(current_segment_index_ + 1);
}

bool HotSnapshotIterator::Valid() const {
  return current_iter_ && current_iter_->Valid();
}

Slice HotSnapshotIterator::key() const { return current_iter_->key(); }
Slice HotSnapshotIterator::value() const { return current_iter_->value(); }
Status HotSnapshotIterator::status() const { 
    if (!status_.ok()) return status_;
    if (current_iter_) return current_iter_->status();
    return Status::OK();
}

void HotSnapshotIterator::SeekToFirst() {
    InitIterForSegment(0);
    if (current_iter_) current_iter_->SeekToFirst();
}

void HotSnapshotIterator::SeekToLast() {
    InitIterForSegment(segments_.size() - 1);
    if (current_iter_) current_iter_->SeekToLast();
}

void HotSnapshotIterator::Prev() {
    if (!current_iter_) return;
    current_iter_->Prev();
    if (!current_iter_->Valid()) {
        SwitchToPrevSegment();
        if (current_iter_) current_iter_->SeekToLast();
    }
}

void HotSnapshotIterator::SwitchToPrevSegment() {
    if (current_segment_index_ > 0) {
        InitIterForSegment(current_segment_index_ - 1);
    } else {
        current_iter_.reset(nullptr);
        current_segment_index_ = -1;
    }
}

void HotSnapshotIterator::SeekForPrev(const Slice& target) {
    // simplified
    Seek(target);
    if (!Valid()) {
        SeekToLast();
    }
    while(Valid() && icmp_.Compare(key(), target) > 0) {
        Prev();
    }
}

}  // namespace ROCKSDB_NAMESPACE