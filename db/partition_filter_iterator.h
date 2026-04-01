#pragma once

#include <cassert>
#include <memory>

#include "table/internal_iterator.h"
#include "util/l0_partition.h"

namespace ROCKSDB_NAMESPACE {

// Iterator wrapper that exposes only keys belonging to a target partition.
class PartitionFilterIterator : public InternalIterator {
 public:
  PartitionFilterIterator(InternalIterator* base, uint32_t partition_id)
      : base_(base), partition_id_(partition_id) {
    assert(base_ != nullptr);
  }

  bool Valid() const override { return base_->Valid(); }

  void SeekToFirst() override {
    base_->SeekToFirst();
    SkipForward();
  }

  void SeekToLast() override {
    base_->SeekToLast();
    SkipBackward();
  }

  void Seek(const Slice& target) override {
    base_->Seek(target);
    SkipForward();
  }

  void SeekForPrev(const Slice& target) override {
    base_->SeekForPrev(target);
    SkipBackward();
  }

  void Next() override {
    if (!base_->Valid()) {
      return;
    }
    base_->Next();
    SkipForward();
  }

  void Prev() override {
    if (!base_->Valid()) {
      return;
    }
    base_->Prev();
    SkipBackward();
  }

  Slice key() const override { return base_->key(); }

  Slice value() const override { return base_->value(); }

  Status status() const override { return base_->status(); }

  bool PrepareValue() override { return base_->PrepareValue(); }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    base_->SetPinnedItersMgr(pinned_iters_mgr);
  }

  bool IsKeyPinned() const override { return base_->IsKeyPinned(); }

  bool IsValuePinned() const override { return base_->IsValuePinned(); }

 private:
  bool KeyMatchesPartition() const {
    return base_->Valid() &&
           ExtractL0PartitionFromInternalKey(base_->key()) == partition_id_;
  }

  void SkipForward() {
    while (base_->Valid() && !KeyMatchesPartition()) {
      base_->Next();
    }
  }

  void SkipBackward() {
    while (base_->Valid() && !KeyMatchesPartition()) {
      base_->Prev();
    }
  }

  std::unique_ptr<InternalIterator> base_;
  uint32_t partition_id_;
};

}  // namespace ROCKSDB_NAMESPACE
