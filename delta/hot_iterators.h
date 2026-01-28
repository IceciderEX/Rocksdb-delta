#pragma once

#include <vector>
#include <memory>
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "db/version_edit.h"       // for FileMetaData
#include "options/cf_options.h"    // for MutableCFOptions
#include "table/internal_iterator.h"
#include "db/table_cache.h"
#include "delta/hot_index_table.h"
#include "delta/hotspot_manager.h"

namespace ROCKSDB_NAMESPACE {

class HotDeltaIterator : public InternalIterator {
 public:
  // deltas: 数据段列表 (FileID, FirstKey, LastKey)
  // icmp: 内部 Key 比较器
  HotDeltaIterator(const std::vector<DataSegment>& deltas,
                   TableCache* table_cache,
                   const ReadOptions& read_options,
                   const FileOptions& file_options,
                   const InternalKeyComparator& icmp,
                   const MutableCFOptions& mutable_cf_options,
                   bool allow_unprepared_value);

  ~HotDeltaIterator() override;

  // InternalIterator 标准接口代理
  bool Valid() const override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void Next() override;
  void Prev() override;
  Slice key() const override;
  Slice value() const override;
  Status status() const override;
  bool PrepareValue() override { return merging_iter_->PrepareValue(); }

 private:
  InternalIterator* merging_iter_;
  std::vector<DataSegment> deltas_;
  // [seg0_lower, seg0_upper, seg1_lower, seg1_upper, ...]
  std::vector<Slice> bounds_slices_;
};


class HotSnapshotIterator : public InternalIterator {
 public:
  HotSnapshotIterator(const std::vector<DataSegment>& segments,
                      HotspotManager* hotspot_manager,
                      TableCache* table_cache,
                      const ReadOptions& read_options,
                      const FileOptions& file_options,
                      const InternalKeyComparator& icmp);

  ~HotSnapshotIterator() override;

  bool Valid() const override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void Next() override;
  void Prev() override;
  Slice key() const override;
  Slice value() const override;
  Status status() const override;

 private:
  // 初始化特定 index 的 segment iterator
  void InitIterForSegment(size_t segment_index);
  
  // 切换到下一个 Segment
  void SwitchToNextSegment();
  void SwitchToPrevSegment();
  
  // 所有现在的snapshot segments
  std::vector<DataSegment> segments_;
  HotspotManager* hotspot_manager_;
  TableCache* table_cache_;
  ReadOptions read_options_;
  FileOptions file_options_;
  const InternalKeyComparator& icmp_;
  const MutableCFOptions mutable_cf_options_;

  // 当前正在使用的 Iterator (指向某个 SST 或 内存 Buffer)
  std::unique_ptr<InternalIterator> current_iter_;
  
  // 当前 Iterator 对应的 Segment 索引
  int current_segment_index_;
  Slice current_lower_bound_;
  Slice current_upper_bound_;
  
  Status status_;
};

}  // namespace ROCKSDB_NAMESPACE