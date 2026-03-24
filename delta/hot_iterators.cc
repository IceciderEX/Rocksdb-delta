#include "delta/hot_iterators.h"

#include "table/merging_iterator.h"
#include "util/cast_util.h"
#include "util/extract_cuid.h"

#include <chrono>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#endif

namespace ROCKSDB_NAMESPACE {
  
static FileMetaData MakeFileMetaFromSegment(const DataSegment& seg, bool is_delta = true) {
  FileMetaData meta;
  // file_number, path_id=0, file_size
  meta.fd = FileDescriptor(seg.file_number, 0, seg.file_size);
  // file_size = 0 对于 delta 来说会 Cache Hit，直接返回 FileReader
  if (seg.file_size <= 0 && !is_delta) {
    std::cerr << "[ERROR] Invalid file size " << seg.file_size
              << " for segment with file number " << seg.file_number
              << std::endl;
  }
  // internal key
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
    : merging_iter_(nullptr), deltas_(deltas) {  // Copy deltas
  if (deltas_.empty()) {
    merging_iter_ = NewEmptyInternalIterator<Slice>(nullptr);
    return;
  }

  bounds_storage_.reserve(deltas_.size() * 2);
  bounds_slices_.reserve(deltas_.size() * 2);
  read_options_storage_.reserve(deltas_.size());

  std::vector<InternalIterator*> children;
  children.reserve(deltas_.size());

  // slices for bounds
  for (const auto& delta : deltas_) {
    bounds_storage_.push_back(ExtractUserKey(delta.first_key).ToString());
    std::string upper = ExtractUserKey(delta.last_key).ToString();
    upper.push_back('\0');  // Make it an exclusive bound
    bounds_storage_.push_back(upper);
  }
  for (size_t i = 0; i < bounds_storage_.size(); ++i) {
    bounds_slices_.emplace_back(bounds_storage_[i]);
  }

  for (size_t i = 0; i < deltas_.size(); ++i) {
    const auto& delta = deltas_[i];
    // ReadOptions
    read_options_storage_.emplace_back(read_options);
    ReadOptions& ro = read_options_storage_.back();
    ro.iterate_lower_bound = &bounds_slices_[i * 2];
    ro.iterate_upper_bound = &bounds_slices_[i * 2 + 1];

    FileMetaData meta = MakeFileMetaFromSegment(delta, true);

    InternalIterator* iter = table_cache->NewIterator(
        ro, file_options, icmp,  // InternalKeyComparator
        meta,                    // FileMetaData
        nullptr, mutable_cf_options, nullptr, nullptr,
        TableReaderCaller::kUserIterator, nullptr, false,
        0,  // L0
        0, nullptr, nullptr, allow_unprepared_value, nullptr, nullptr);

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
void HotDeltaIterator::Seek(const Slice& target) {
  merging_iter_->Seek(target);
}
void HotDeltaIterator::SeekForPrev(const Slice& target) {
  merging_iter_->SeekForPrev(target);
}
void HotDeltaIterator::Next() { merging_iter_->Next(); }
void HotDeltaIterator::Prev() { merging_iter_->Prev(); }
Slice HotDeltaIterator::key() const { return merging_iter_->key(); }
Slice HotDeltaIterator::value() const { return merging_iter_->value(); }
Status HotDeltaIterator::status() const { return merging_iter_->status(); }
uint64_t HotDeltaIterator::GetPhysicalId() {
  return merging_iter_->GetPhysicalId();
}

namespace {

class BoundedMemIterator : public InternalIterator {
 public:
  BoundedMemIterator(InternalIterator* iter, const std::string& lower_bound,
                     const std::string& upper_bound,
                     const InternalKeyComparator* icmp)
      : iter_(iter),
        lower_bound_(lower_bound),
        upper_bound_(upper_bound),
        icmp_(icmp) {
    UpdateValid();
  }

  ~BoundedMemIterator() override { delete iter_; }

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    if (!lower_bound_.empty()) {
      iter_->Seek(lower_bound_);
    } else {
      iter_->SeekToFirst();
    }
    UpdateValid();
  }

  void SeekToLast() override {
    if (!upper_bound_.empty()) {
      iter_->SeekForPrev(upper_bound_);
    } else {
      iter_->SeekToLast();
    }
    UpdateValid();
  }

  void Seek(const Slice& target) override {
    if (!lower_bound_.empty() && icmp_->Compare(target, lower_bound_) < 0) {
      iter_->Seek(lower_bound_);
    } else if (!upper_bound_.empty() && icmp_->Compare(target, upper_bound_) > 0) {
      valid_ = false;
      return;
    } else {
      iter_->Seek(target);
    }
    UpdateValid();
  }

  void SeekForPrev(const Slice& target) override {
    if (!upper_bound_.empty() && icmp_->Compare(target, upper_bound_) > 0) {
      iter_->SeekForPrev(upper_bound_);
    } else if (!lower_bound_.empty() && icmp_->Compare(target, lower_bound_) < 0) {
      valid_ = false;
      return;
    } else {
      iter_->SeekForPrev(target);
    }
    UpdateValid();
  }

  void Next() override {
    iter_->Next();
    UpdateValid();
  }

  void Prev() override {
    iter_->Prev();
    UpdateValid();
  }

  Slice key() const override { return iter_->key(); }
  Slice value() const override { return iter_->value(); }
  Status status() const override { return iter_->status(); }
  
  bool PrepareValue() override { return iter_->PrepareValue(); }

 private:
  void UpdateValid() {
    valid_ = iter_->Valid() && IsWithinBounds(iter_->key());
  }

  bool IsWithinBounds(const Slice& k) const {
    if (!lower_bound_.empty() && icmp_->Compare(k, lower_bound_) < 0) return false;
    if (!upper_bound_.empty() && icmp_->Compare(k, upper_bound_) > 0) return false;
    return true;
  }

  InternalIterator* iter_;
  std::string lower_bound_;
  std::string upper_bound_;
  const InternalKeyComparator* icmp_;
  bool valid_ = false;
};

} // namespace

// ====================== HotSnapshotIterator ============================

HotSnapshotIterator::HotSnapshotIterator(
    const std::vector<DataSegment>& segments, uint64_t cuid,
    HotspotManager* hotspot_manager, TableCache* table_cache,
    const ReadOptions& read_options, const FileOptions& file_options,
    const InternalKeyComparator& icmp,
    const MutableCFOptions& mutable_cf_options)
    : segments_(segments),
      cuid_(cuid),
      hotspot_manager_(hotspot_manager),
      table_cache_(table_cache),
      read_options_(read_options),
      file_options_(file_options),
      icmp_(icmp),
      mutable_cf_options_(mutable_cf_options),
      current_segment_index_(-1),
      status_(Status::OK()) {}

HotSnapshotIterator::~HotSnapshotIterator() {
  // current_iter_ unique_ptr auto released
}

bool HotSnapshotIterator::InitIterForSegment(size_t index) {
  if (index >= segments_.size()) {
    current_iter_.reset(nullptr);
    current_segment_index_ = -1;
    return false;
  }

  const auto& seg = segments_[index];
  current_segment_index_ = static_cast<int>(index);

  if (seg.file_number == static_cast<uint64_t>(-1)) {
    // Case A: 内存 Buffer fileid -1
    InternalIterator* mem_iter =
        hotspot_manager_->NewBufferIterator(cuid_, &icmp_);
    
    // 防止buffer data在线程调度时清空，再检查一次
    mem_iter->SeekToFirst();
    if (!mem_iter->Valid()) {
      HotIndexEntry latest_entry;
      if (hotspot_manager_->GetHotIndexEntry(cuid_, &latest_entry) && latest_entry.HasSnapshot()) {
        bool snapshot_changed = false;
        if (segments_.size() != latest_entry.snapshot_segments.size()) {
           snapshot_changed = true;
        } else {
           for (size_t i = 0; i < segments_.size(); ++i) {
               if (segments_[i].file_number != latest_entry.snapshot_segments[i].file_number) {
                   snapshot_changed = true;
                   break;
               }
           }
        }
        if (snapshot_changed) {
          segments_ = latest_entry.snapshot_segments;
          delete mem_iter;
          return true;  // index again
        }
      }
    }
    current_iter_.reset(mem_iter);
    // 使用 boundedIter，防止访问越界?
    // current_iter_.reset(new BoundedMemIterator(mem_iter, seg.first_key, seg.last_key, &icmp_));
    return false;
  } else {
    // Case B: 物理 SST
    FileMetaData meta = MakeFileMetaFromSegment(seg, false);

    current_read_options_ = read_options_;
    current_lower_bound_str_ = ExtractUserKey(seg.first_key).ToString();
    current_upper_bound_str_ = ExtractUserKey(seg.last_key).ToString();
    current_upper_bound_str_.push_back('\0');  // Make it an exclusive bound
    current_lower_bound_slice_ = Slice(current_lower_bound_str_);
    current_upper_bound_slice_ = Slice(current_upper_bound_str_);
    current_read_options_.iterate_lower_bound = &current_lower_bound_slice_;
    current_read_options_.iterate_upper_bound = &current_upper_bound_slice_;

    InternalIterator* iter = table_cache_->NewIterator(
        current_read_options_, file_options_, icmp_, meta, nullptr,
        mutable_cf_options_, nullptr, nullptr, TableReaderCaller::kUserIterator,
        nullptr, false,
        1,  // L1+
        0, nullptr, nullptr,
        false,  // allow_unprepared_value
        nullptr, nullptr);

    current_iter_.reset(iter);
    return false;
  }
}

void HotSnapshotIterator::Seek(const Slice& target) {
  if (segments_.empty()) {
    current_iter_.reset(nullptr);
    return;
  }

  while (true) {
    // EndKey >= Target 的 Segment
    auto it = std::lower_bound(
        segments_.begin(), segments_.end(), target,
        [&](const DataSegment& seg, const Slice& val) {
          if (seg.last_key.empty()) {
            fprintf(
                stderr,
                "Warning: Empty last_key in segment. This should not happen.\n");
            return false;
          }
          // 比较 seg.last_key < val
          return icmp_.user_comparator()->Compare(ExtractUserKey(seg.last_key),
                                                  ExtractUserKey(val)) < 0;
        });

    size_t index = std::distance(segments_.begin(), it);

    if (it == segments_.end()) {
      current_iter_.reset(nullptr);
      current_segment_index_ = -1;
      return;
    }

    if (static_cast<int>(index) != current_segment_index_) {
      if (InitIterForSegment(index)) { // if true, index again
        current_segment_index_ = -1; // 必须重置，否则下一轮循环中若 index 碰巧相同则会跳过 Init
        continue;
      }
    }
    
    break;
  }

  // segment seek
  if (current_iter_) {
    current_iter_->Seek(target);

    if (!Valid()) {
      SwitchToNextSegment();
      if (Valid()) {
        current_iter_->Seek(segments_[current_segment_index_].first_key);
      }
    }
  }
}

void HotSnapshotIterator::Next() {
  if (!current_iter_) return;

  std::string prev_key_debug = key().ToString();

  current_iter_->Next();

  if (!Valid()) {
    // 当前 Segment 耗尽(或越界)，切换到下一个
    int old_segment = current_segment_index_;
    SwitchToNextSegment();
    if (Valid()) {
      current_iter_->Seek(segments_[current_segment_index_].first_key);
    }
    
    // // Debug 打印跨段情况，捕捉跨段回退
    // if (Valid() && icmp_.Compare(Slice(prev_key_debug), key()) > 0) {
    //   fprintf(stderr, "[FATAL] HotSnapshotIterator Regression across segment!\n");
    //   fprintf(stderr, "Prev: %s", FormatKeyDisplay(prev_key_debug).c_str());
    //   fprintf(stderr, "Curr: %s", FormatKeyDisplay(key()).c_str());
    //   fprintf(stderr, "Old Segment's info: file_number=%lu, first_key=%s, last_key=%s\n",
    //           segments_[old_segment].file_number,
    //           FormatKeyDisplay(segments_[old_segment].first_key).c_str(),
    //           FormatKeyDisplay(segments_[old_segment].last_key).c_str());
    //   fprintf(stderr, "New Segment's info: file_number=%lu, first_key=%s, last_key=%s\n",
    //           segments_[current_segment_index_].file_number,
    //           FormatKeyDisplay(segments_[current_segment_index_].first_key).c_str(),
    //           FormatKeyDisplay(segments_[current_segment_index_].last_key).c_str());
    //   fprintf(stderr, "Old Segment: %d, New Segment: %d\n", old_segment, current_segment_index_);
    //   assert(true);
    // }
  } else {
    // Debug 打印同段情况，捕捉同段回退
    // if (icmp_.Compare(Slice(prev_key_debug), key()) >= 0) {
    //   fprintf(stderr, "Prev: %s", FormatKeyDisplay(prev_key_debug).c_str());
    //   fprintf(stderr, "Curr: %s", FormatKeyDisplay(key()).c_str());
    //   fprintf(stderr, "[FATAL] HotSnapshotIterator Regression within segment %d!\n", current_segment_index_);
    //   assert(true);
    // }
  }
}

void HotSnapshotIterator::SwitchToNextSegment() {
  int target_index = current_segment_index_ + 1;
  while (InitIterForSegment(target_index)) {
  }
}

bool HotSnapshotIterator::Valid() const {
  if (!current_iter_ || !current_iter_->Valid()) {
    return false;
  }

  // 边界检查：需要在 Segment 范围内
  const auto& seg = segments_[current_segment_index_];
  bool ret = true;
  if (!seg.last_key.empty()) {
    if (icmp_.user_comparator()->Compare(ExtractUserKey(current_iter_->key()),
                                         ExtractUserKey(seg.last_key)) > 0) {
      ret = false;
    }
  }
  return ret;
}

Slice HotSnapshotIterator::key() const { return current_iter_->key(); }
Slice HotSnapshotIterator::value() const { return current_iter_->value(); }
Status HotSnapshotIterator::status() const {
  if (!status_.ok()) return status_;
  if (current_iter_) return current_iter_->status();
  return Status::OK();
}

void HotSnapshotIterator::SeekToFirst() {
  while (InitIterForSegment(0)) {
  }
  if (current_iter_) current_iter_->Seek(segments_[current_segment_index_].first_key);
}

void HotSnapshotIterator::SeekToLast() {
  while (InitIterForSegment(segments_.size() - 1)) {
  }
  if (current_iter_) {
    if (segments_[current_segment_index_].file_number == static_cast<uint64_t>(-1)) {
      current_iter_->SeekForPrev(segments_[current_segment_index_].last_key);
    } else {
      current_iter_->SeekToLast();
    }
  }
}

void HotSnapshotIterator::Prev() {
  if (!current_iter_) return;
  current_iter_->Prev();
  if (!Valid()) {
    SwitchToPrevSegment();
    if (Valid()) {
      if (segments_[current_segment_index_].file_number == static_cast<uint64_t>(-1)) {
        current_iter_->SeekForPrev(segments_[current_segment_index_].last_key);
      } else {
        current_iter_->SeekToLast();
      }
    }
  }
}

void HotSnapshotIterator::SwitchToPrevSegment() {
  if (current_segment_index_ > 0) {
    int target_index = current_segment_index_ - 1;
    while (InitIterForSegment(target_index)) {
    }
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
  while (Valid() && icmp_.Compare(key(), target) > 0) {
    Prev();
  }
}

uint64_t HotSnapshotIterator::GetPhysicalId() {
  if (current_iter_) return current_iter_->GetPhysicalId();
  return 0;
}

// ===================================================================
// DeltaSwitchingIterator Implementation
// ===================================================================

DeltaSwitchingIterator::DeltaSwitchingIterator(
    Version* version, HotspotManager* hotspot_manager,
    const ReadOptions& read_options, const FileOptions& file_options,
    const InternalKeyComparator& icmp,
    const MutableCFOptions& mutable_cf_options, Arena* arena)
    : version_(version),
      hotspot_manager_(hotspot_manager),
      read_options_(read_options),
      file_options_(file_options),
      icmp_(icmp),
      mutable_cf_options_(mutable_cf_options),
      arena_(arena),
      current_iter_(nullptr),
      cold_iter_(nullptr),
      hot_iter_(nullptr),
      current_hot_cuid_(0),
      is_hot_mode_(false) {
}

DeltaSwitchingIterator::~DeltaSwitchingIterator() {
  if (hot_iter_) {
    delete hot_iter_;
  }
  // rocksdb 会在 arena 中分配内存，不需要手动删除
  if (cold_iter_) {
    if (arena_) {
      cold_iter_->~InternalIterator();
    } else {
      delete cold_iter_;
    }
  }
}

void DeltaSwitchingIterator::InitColdIter() {
  if (cold_iter_) return;

  // L0~Ln 所有文件的 MergingIterator
  // Arena=nullptr, skip_filters=false
  MergeIteratorBuilder builder(&icmp_, arena_);
  version_->AddIterators(read_options_, file_options_, &builder,
                         /*allow_unprepared*/ false);
  // get MergingIterator
  cold_iter_ = builder.Finish();

  if (!cold_iter_) {
    cold_iter_ = NewEmptyInternalIterator<Slice>(arena_);
  }
}

bool DeltaSwitchingIterator::InitHotIter(uint64_t cuid) {
  if (hot_iter_ && current_hot_cuid_ == cuid) return true;

  if (hot_iter_) {
    delete hot_iter_;
    hot_iter_ = nullptr;
  }

  // 1. 获取元数据
  HotIndexEntry entry;
  if (!hotspot_manager_->GetHotIndexEntry(cuid, &entry) || !entry.HasSnapshot()) {
    // 如果没有 Snapshot，使用 cold path
    return false;
  }

  // snapshot and delta
  InternalIterator* snapshot_iter =
      new HotSnapshotIterator(entry.snapshot_segments, cuid, hotspot_manager_,
                              version_->cfd()->table_cache(), read_options_,
                              file_options_, icmp_, mutable_cf_options_);

  InternalIterator* delta_iter = new HotDeltaIterator(
        entry.deltas, version_->cfd()->table_cache(), read_options_,
      file_options_, icmp_, mutable_cf_options_, false);

  std::vector<InternalIterator*> children = {delta_iter, snapshot_iter};
  hot_iter_ = NewMergingIterator(&icmp_, children.data(), 2);
  current_hot_cuid_ = cuid;
  return true;
}

void DeltaSwitchingIterator::Seek(const Slice& target) {
  uint64_t cuid = hotspot_manager_->ExtractCUID(target);

  // bool use_hot = false;
  // // hot cuid
  // if (!read_options_.skip_hot_path && !read_options_.delta_full_scan &&
  //     cuid != 0 && hotspot_manager_->IsHot(cuid)) {
  //   use_hot = true;
  // }

  // if (use_hot) {
  //   InitHotIter(cuid);
  //   current_iter_ = hot_iter_;
  //   is_hot_mode_ = true;
  // } else {
  //   InitColdIter();
  //   current_iter_ = cold_iter_;
  //   is_hot_mode_ = false;
  // }
  // skip_hot_path (eg. Metadata Scan)，强制冷路径
  if (read_options_.skip_hot_path) {
    InitColdIter();
    current_iter_ = cold_iter_;
    is_hot_mode_ = false;
  }
  // 否则，不论是点查还是全扫描(delta_full_scan)，只要是热点，全部走热点路径提供给用户！
  else if (cuid != 0 && hotspot_manager_->IsHot(cuid)) {
    if (InitHotIter(cuid)) {
      current_iter_ = hot_iter_;
      is_hot_mode_ = true;
    } else {
      // 热点索引尚未建立（如 Init Scan 还在进行中），回退到 Cold Path
      InitColdIter();
      current_iter_ = cold_iter_;
      is_hot_mode_ = false;
    }
  }
  // 既不是 skip_hot_path 也不是热点，默认走冷路径
  else {
    InitColdIter();
    current_iter_ = cold_iter_;
    is_hot_mode_ = false;
  }

  if (current_iter_) {
    current_iter_->Seek(target);
  }
}

// 全表扫描或未知方向，强制回退到 Cold Mode
void DeltaSwitchingIterator::SeekToFirst() {
  if (read_options_.skip_hot_path) {
    InitColdIter();
    current_iter_ = cold_iter_;
    is_hot_mode_ = false;
  }
  // InitColdIter();
  // current_iter_ = cold_iter_;
  // is_hot_mode_ = false;
  if (current_iter_) current_iter_->SeekToFirst();
}

void DeltaSwitchingIterator::SeekToLast() {
  InitColdIter();
  current_iter_ = cold_iter_;
  is_hot_mode_ = false;
  if (current_iter_) current_iter_->SeekToLast();
}

bool DeltaSwitchingIterator::Valid() const {
  return current_iter_ && current_iter_->Valid();
}
// void DeltaSwitchingIterator::Next() {
//   if (current_iter_) current_iter_->Next();
// }

void DeltaSwitchingIterator::Next() {
  if (!current_iter_) return;

  if (is_hot_mode_ && current_iter_->Valid()) {
    Slice cur_key = current_iter_->key();
    Slice prev_user_key = ExtractUserKey(cur_key);
    prev_user_key_buf_.assign(prev_user_key.data(), prev_user_key.size());

    current_iter_->Next();
    
    // 跳过与上一条相同 UserKey 的所有条目
    while (current_iter_->Valid()) {
      Slice cur_user_key = ExtractUserKey(current_iter_->key());
      int cmp = icmp_.user_comparator()->Compare(cur_user_key, Slice(prev_user_key_buf_));

      if (cmp != 0) {
        break;
      }

      current_iter_->Next();
    }
  } else {
    current_iter_->Next();
  }
}

void DeltaSwitchingIterator::Prev() {
  if (current_iter_) current_iter_->Prev();
}
void DeltaSwitchingIterator::SeekForPrev(const Slice& target) {
  // 逻辑同 Seek
  Seek(target);
  if (!Valid()) SeekToLast();
  while (Valid() && icmp_.Compare(key(), target) > 0) Prev();
}

Slice DeltaSwitchingIterator::key() const { return current_iter_->key(); }
Slice DeltaSwitchingIterator::value() const { return current_iter_->value(); }
Status DeltaSwitchingIterator::status() const {
  if (current_iter_) return current_iter_->status();
  return Status::OK();
}
bool DeltaSwitchingIterator::PrepareValue() {
  if (current_iter_) return current_iter_->PrepareValue();
  return false;
}
uint64_t DeltaSwitchingIterator::GetPhysicalId() {
  if (current_iter_) return current_iter_->GetPhysicalId();
  return 0;
}

}  // namespace ROCKSDB_NAMESPACE