#include "delta/data_segment_iterator.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/format.h" // For BlockHandle

namespace ROCKSDB_NAMESPACE {

DataSegmentIterator::DataSegmentIterator(BlockBasedTable* table,
                                         const ReadOptions& read_options,
                                         const InternalKeyComparator& icomp,
                                         const std::string& start_key,
                                         const std::string& end_key)
    : table_(table),
      read_options_(read_options),
      icomp_(icomp),
      start_key_(start_key),
      end_key_(end_key),
      valid_(false) {}

void DataSegmentIterator::CreateIndexIteratorIfNeeded() {
  if (!index_iter_) {
    // need_upper_bound_check = false
    index_iter_.reset(table_->NewIndexIterator(read_options_, false));
  }
}

void DataSegmentIterator::InitDataBlock() {
  data_iter_.reset(); // 清理旧的 Data Block 迭代器

  if (!index_iter_ || !index_iter_->Valid()) {
    valid_ = false;
    return;
  }

  // 从 Index Entry 中解码 BlockHandle
  BlockHandle handle;
  Slice input = index_iter_->value();
  Status s = handle.DecodeFrom(&input);
  
  if (!s.ok()) {
    status_ = s;
    valid_ = false;
    return;
  }

  // 加载 Data Block
  // 注意：BlockBasedTable::NewDataBlockIterator 是最核心的接口
  data_iter_.reset(table_->NewDataBlockIterator(read_options_, handle));
  
  if (!data_iter_) {
    status_ = Status::Corruption("Could not create data block iterator");
    valid_ = false;
  }
}

bool DataSegmentIterator::Valid() const {
  return valid_ && data_iter_ && data_iter_->Valid();
}

void DataSegmentIterator::SeekToFirst() {
  // SeekToFirst 本质上就是 Seek(start_key_)
  Seek(start_key_);
}

void DataSegmentIterator::Seek(const Slice& target) {
  status_ = Status::OK();
  valid_ = false; // 先置为无效，成功后置为有效

  // 1. 范围检查与修正
  // 如果 target 小于 start_key_，则我们需要从 start_key_ 开始读
  int cmp_start = icomp_.Compare(target, start_key_);
  const Slice& actual_target = (cmp_start < 0) ? start_key_ : target;

  // 如果 target 已经大于 end_key_，直接无效
  if (icomp_.Compare(actual_target, end_key_) > 0) {
    return;
  }

  // 2. 初始化 Index Iterator 并定位 Data Block
  CreateIndexIteratorIfNeeded();
  
  // Index Seek: 二分查找找到包含 actual_target 的 Block
  index_iter_->Seek(actual_target);

  if (!index_iter_->Valid()) {
    return; // 没找到任何 Block
  }

  // 3. 加载 Data Block 并 Seek
  InitDataBlock();
  
  if (data_iter_) {
    data_iter_->Seek(actual_target);
    
    // 如果 Block 内 Seek 到底了（例如 target 比该 Block 所有 key 都大），
    // 或者是为了应对 Bloom Filter 过滤后的情况，
    // 我们可能需要去下一个 Block 看看
    if (!data_iter_->Valid()) {
       AdvanceToNextBlock();
    }
  }

  // 4. 再次检查边界 (Double Check)
  // AdvanceToNextBlock 后可能已经超出了 end_key_
  if (data_iter_ && data_iter_->Valid()) {
    if (icomp_.Compare(data_iter_->key(), end_key_) > 0) {
      valid_ = false;
    } else {
      valid_ = true;
    }
  }
}

void DataSegmentIterator::Next() {
  if (!Valid()) return;

  // 1. 在当前 Data Block 内推进
  data_iter_->Next();

  // 2. 如果当前 Block 读完了，去下一个 Block
  if (!data_iter_->Valid()) {
    AdvanceToNextBlock();
  }

  // 3. 检查是否越过了 end_key_
  if (data_iter_ && data_iter_->Valid()) {
    if (icomp_.Compare(data_iter_->key(), end_key_) > 0) {
      valid_ = false;
    } else {
      valid_ = true;
    }
  } else {
    valid_ = false;
  }
}

void DataSegmentIterator::AdvanceToNextBlock() {
  // Index 往后移，找下一个 Block
  index_iter_->Next();
  
  if (index_iter_->Valid()) {
    InitDataBlock();
    if (data_iter_) {
      data_iter_->SeekToFirst();
    }
  } else {
    // 没有更多 Block 了
    data_iter_.reset();
    valid_ = false;
  }
}

Slice DataSegmentIterator::key() const {
  // 必须保证 Valid() 为 true 才能调用
  return data_iter_->key();
}

Slice DataSegmentIterator::value() const {
  return data_iter_->value();
}

Status DataSegmentIterator::status() const {
  if (!status_.ok()) return status_;
  if (data_iter_ && !data_iter_->status().ok()) return data_iter_->status();
  if (index_iter_ && !index_iter_->status().ok()) return index_iter_->status();
  return Status::OK();
}

// --- 不支持或简化的反向操作 ---

void DataSegmentIterator::SeekForPrev(const Slice& target) {
  // 简单策略：降级为 Seek。
  // 对于 Scan 场景，SeekForPrev 很少被调用，除非是 Reverse Scan。
  // 如果必须支持 Reverse Scan，这里的逻辑会复杂很多 (需要 Prev Index Block)。
  Seek(target);
}

void DataSegmentIterator::SeekToLast() {
  // 暂不支持
  status_ = Status::NotSupported("DataSegmentIterator::SeekToLast");
  valid_ = false;
}

void DataSegmentIterator::Prev() {
  // 暂不支持
  status_ = Status::NotSupported("DataSegmentIterator::Prev");
  valid_ = false;
}

}  // namespace ROCKSDB_NAMESPACE