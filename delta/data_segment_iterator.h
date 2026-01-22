#pragma once

#include <string>
#include <memory>
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"
#include "table/block_based/block_based_table_reader.h"

namespace ROCKSDB_NAMESPACE {

class DataSegmentIterator : public InternalIterator {
 public:
  // 构造函数
  // table: 对应的 SST 文件 Reader
  // icomp: 内部 Key 比较器 (用于比较 StartKey/EndKey)
  // start_key: 该 Segment 的起始 Key (Inclusive)
  // end_key: 该 Segment 的结束 Key (Inclusive)
  DataSegmentIterator(BlockBasedTable* table,
                      const ReadOptions& read_options,
                      const InternalKeyComparator& icomp,
                      const std::string& start_key,
                      const std::string& end_key);

  virtual ~DataSegmentIterator() = default;

  // 禁止拷贝和赋值
  DataSegmentIterator(const DataSegmentIterator&) = delete;
  DataSegmentIterator& operator=(const DataSegmentIterator&) = delete;

  // --- InternalIterator 核心接口实现 ---

  bool Valid() const override;
  
  // 利用 Index Block 二分查找定位到 StartKey
  void SeekToFirst() override;
  
  // 不支持 SeekToLast (因为只服务于正向 Scan)
  void SeekToLast() override;
  
  // 利用 Index Block 二分查找定位 Target
  void Seek(const Slice& target) override;
  
  // 简化的反向 Seek，回退到 Seek()
  void SeekForPrev(const Slice& target) override;
  
  void Next() override;
  
  // 暂不支持 Prev (只服务于正向 Scan)
  void Prev() override;
  
  Slice key() const override;
  Slice value() const override;
  Status status() const override;

 private:
  // 辅助函数：初始化 Index Iterator (懒加载)
  void CreateIndexIteratorIfNeeded();
  
  // 辅助函数：根据 Index Iterator 当前指向的 Handle 加载 Data Block
  void InitDataBlock();
  
  // 辅助函数：当一个 Data Block 读完后，移动到下一个
  void AdvanceToNextBlock();

  // 成员变量
  BlockBasedTable* table_;
  ReadOptions read_options_;
  const InternalKeyComparator& icomp_;
  
  std::string start_key_;
  std::string end_key_;

  // 双层迭代器结构
  std::unique_ptr<InternalIterator> index_iter_; // L1: 遍历 Index Block
  std::unique_ptr<InternalIterator> data_iter_;  // L2: 遍历 Data Block
  
  Status status_;
  bool valid_; // 缓存 Valid 状态，用于快速判断边界
};

}  // namespace ROCKSDB_NAMESPACE