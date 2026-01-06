#pragma once

#include <vector>
#include <memory>
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/sst_file_reader.h"
#include "table/merging_iterator.h"
#include "delta/hot_index_table.h" 
#include "delta/hot_data_buffer.h" // for HotSstLifecycleManager

namespace ROCKSDB_NAMESPACE {

class HotspotManager; // Forward declaration if needed

class HotSnapshotIterator : public InternalIterator {
 public:
  // options: 读取 SST 的选项
  HotSnapshotIterator(uint64_t cuid, 
                      const std::vector<DataSegment>& segments,
                      std::shared_ptr<HotSstLifecycleManager> lifecycle_manager,
                      const Options& options);

  ~HotSnapshotIterator() override = default;

  // InternalIterator
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

  bool IsKeyPinned() const override { return false; }
  bool IsValuePinned() const override { return false; }

 private:
  // 初始化或切换到 index 指定的 Segment
  void LoadSegment(size_t index);
  
  // 检查当前物理迭代器是否指向了正确 CUID 的数据
  // 如果当前物理迭代器耗尽或越界（读到了其他 CUID），尝试加载下一个 Segment
  void AdvanceToNextValidSegment();

  // 构造用于 Seek 的最小 Key (CUID 前缀)
  std::string MakeSeekKey(uint64_t cuid);

  uint64_t ExtractCUID(const Slice& key) const;

 private:
  uint64_t cuid_;
  std::vector<DataSegment> segments_;
  std::shared_ptr<HotSstLifecycleManager> lifecycle_manager_;
  Options options_;

  size_t current_segment_index_;
  
  // sst Reader 和 Iterator
  std::unique_ptr<SstFileReader> current_file_reader_;
  std::unique_ptr<Iterator> current_file_iter_;
  
  Status status_;
  bool valid_;
};

}  // namespace ROCKSDB_NAMESPACE