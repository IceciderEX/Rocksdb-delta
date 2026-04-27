//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/flush_job.h"

#include <algorithm>
#include <cinttypes>
#include <vector>

#include "db/builder.h"
#include "db/delta_l0.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "logging/event_logger.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "table/two_level_iterator.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Delta flush 专用的分区迭代器。
// 
// 作用：
// 在 flush memtable 生成 L0 文件时，将 base iterator 中的数据按
// Delta L0 partition 分段输出，确保一次 flush 可以按 partition 边界
// 拆成多个 SST 文件，而不是把多个 partition 的 key 混在同一个 L0 文件中。
//
// 工作方式：
// 1. 外层 flush 逻辑创建该 iterator 包装原始 InternalIterator；
// 2. SeekToFirst() 定位到当前待 flush 数据的第一条 key；
// 3. 根据第一条 key 的 tableid 查询/注册所属 Delta L0 partition；
// 4. Next() 只在当前 partition 范围内推进；
// 5. 一旦遇到不属于 current_partition_ 的 tableid，valid_ 变为 false；
// 6. 外层 flush 结束当前 SST 文件后，可以再次调用 SeekToFirst() / 后续流程
//    从 base_iter_ 当前停留的位置继续处理下一个 partition。
class DeltaPartitionedFlushIterator : public InternalIterator {
 public:
  // 构造 Delta partition-aware flush iterator。
  //
  // base_iter 是原始 memtable/internal iterator，数据读取仍由它完成；
  // directory 用于根据 tableid 查找、注册和动态 split Delta L0 partition。
  DeltaPartitionedFlushIterator(InternalIterator* base_iter,
                                DeltaL0PartitionDirectory* directory)
      : base_iter_(base_iter), directory_(directory) {}

  // 当前 iterator 是否有效。
  // 只有内部状态正常且当前 key 仍属于当前 partition 时才返回 true。
  bool Valid() const override { return status_.ok() && valid_; }

  // 定位到当前分区段的第一条记录。
  //
  // 第一次调用时会将 base_iter_ SeekToFirst()；
  // 后续调用不会重置 base_iter_，而是从上一次停止的位置继续，
  // 这样可以让 flush 逻辑逐个 partition 地生成多个输出文件。
  void SeekToFirst() override {
    status_ = Status::OK();
    valid_ = false;
    if (!initialized_) {
      base_iter_->SeekToFirst();
      initialized_ = true;
    }
    if (!base_iter_->status().ok()) {
      status_ = base_iter_->status();
      return;
    }
    PrepareCurrentPartition();
  }

  // 不支持反向定位到最后一条记录。
  // 该 iterator 只服务于 flush 顺序写文件场景，因此只需要正向遍历。
  void SeekToLast() override {
    status_ = Status::NotSupported("DeltaPartitionedFlushIterator::SeekToLast");
    valid_ = false;
  }

  // 不支持任意 Seek。
  // flush 过程中要求从当前位置顺序扫描，避免破坏按 partition 分段输出的状态。
  void Seek(const Slice& /*target*/) override {
    status_ = Status::NotSupported("DeltaPartitionedFlushIterator::Seek");
    valid_ = false;
  }

  // 不支持反向 Seek。
  void SeekForPrev(const Slice& /*target*/) override {
    status_ =
        Status::NotSupported("DeltaPartitionedFlushIterator::SeekForPrev");
    valid_ = false;
  }

  // 前进到下一条记录。
  // 如果下一条记录仍属于 current_partition_，iterator 保持有效；
  // 如果跨越到其他 partition，则 valid_ 变为 false，
  // 外层 flush 可以结束当前输出文件并开始下一个 partition 文件。
  void Next() override {
    assert(valid_);
    base_iter_->Next();
    AdvanceWithinPartition();
  }

  // 不支持反向遍历。
  void Prev() override {
    status_ = Status::NotSupported("DeltaPartitionedFlushIterator::Prev");
    valid_ = false;
  }

  // 返回当前 key。
  // 实际 key 数据来自底层 base_iter_。
  Slice key() const override {
    assert(valid_);
    return base_iter_->key();
  }

  // 返回当前 value。
  // 实际 value 数据来自底层 base_iter_。
  Slice value() const override {
    assert(valid_);
    return base_iter_->value();
  }

  // 返回 iterator 状态。
  // 优先返回本包装 iterator 自己记录的错误；
  // 如果自身状态正常，则透传 base_iter_ 的状态。
  Status status() const override {
    if (!status_.ok()) {
      return status_;
    }
    return base_iter_->status();
  }

  // 准备当前 value。
  // value 的实际加载仍交给底层 base_iter_。
  bool PrepareValue() override {
    return base_iter_->PrepareValue();
  }

  // 返回当前输出段对应的 Delta L0 partition。
  // flush 结束当前 SST 文件时，可以把该 partition 的 partition_id
  // 和 generation 写入输出文件的 FileMetaData::delta_l0_meta。
  const DeltaL0Partition& current_partition() const {
    return current_partition_;
  }

  // 判断底层 base_iter_ 是否还有尚未 flush 的数据。
  //
  // 当当前 partition 段结束时，本 iterator 的 Valid() 可能为 false，
  // 但 base_iter_ 仍然可能停在下一个 partition 的第一条 key 上；
  // 外层逻辑可通过该函数判断是否需要继续打开下一个输出文件。
  bool HasPendingData() const {
    return status_.ok() && base_iter_->Valid();
  }

 private:
  // 根据 base_iter_ 当前 key 初始化当前 partition。
  //
  // 该函数会：
  // 1. 从 internal key 中提取 user key；
  // 2. 从 Delta user key 中解析 tableid；
  // 3. 在 DeltaL0PartitionDirectory 中查找并注册该 tableid；
  // 4. 将查到的 partition 保存为 current_partition_；
  // 5. 设置 last_tableid_，用于后续避免重复 Observe 同一个 tableid。
  void PrepareCurrentPartition() {
    if (!base_iter_->Valid()) {
      valid_ = false;
      return;
    }

    uint64_t tableid = 0;
    if (!TryParseDeltaTableIdFromUserKey(ExtractUserKey(base_iter_->key()),
                                         &tableid)) {
      status_ = Status::InvalidArgument("Invalid Delta user key during flush");
      valid_ = false;
      return;
    }

    current_partition_ = directory_->LookupAndRegister(tableid);
    last_tableid_ = tableid;
    valid_ = true;
  }

  // 在当前 partition 内推进。
  //
  // 如果下一条 key 仍属于 current_partition_，则继续保持 valid；
  // 如果下一条 key 属于其他 partition，则将 valid_ 置为 false，
  // 表示当前输出文件的 partition 段已经结束。
  void AdvanceWithinPartition() {
    if (!base_iter_->status().ok()) {
      status_ = base_iter_->status();
      valid_ = false;
      return;
    }
    if (!base_iter_->Valid()) {
      valid_ = false;
      return;
    }

    uint64_t tableid = 0;
    if (!TryParseDeltaTableIdFromUserKey(ExtractUserKey(base_iter_->key()),
                                         &tableid)) {
      status_ = Status::InvalidArgument("Invalid Delta user key during flush");
      valid_ = false;
      return;
    }

    if (tableid != last_tableid_) {
      directory_->Observe(tableid);
      last_tableid_ = tableid;
    }

    valid_ = current_partition_.Contains(tableid);
  }

  // 被包装的底层 iterator，负责实际遍历 memtable/internal key-value。
  InternalIterator* base_iter_;
  // Delta L0 partition directory，用于根据 tableid 查找、注册和 split partition。
  DeltaL0PartitionDirectory* directory_;
  // 当前 flush 输出文件对应的 Delta L0 partition。
  DeltaL0Partition current_partition_;
  // 包装 iterator 自身记录的错误状态。
  Status status_;
  // 是否已经对 base_iter_ 执行过第一次 SeekToFirst。
  // 后续 SeekToFirst 不会重头开始，而是从 base_iter_ 当前停留位置继续。
  bool initialized_ = false;
  // 当前包装 iterator 是否有效。
  // valid_ 为 false 可能表示底层耗尽，也可能表示跨出了当前 partition。
  bool valid_ = false;
  // 上一次观察到的 tableid。
  // 用于避免连续相同 tableid 重复调用 directory_->Observe()。
  uint64_t last_tableid_ = 0;
};

}  // namespace

const char* GetFlushReasonString(FlushReason flush_reason) {
  switch (flush_reason) {
    case FlushReason::kOthers:
      return "Other Reasons";
    case FlushReason::kGetLiveFiles:
      return "Get Live Files";
    case FlushReason::kShutDown:
      return "Shut down";
    case FlushReason::kExternalFileIngestion:
      return "External File Ingestion";
    case FlushReason::kManualCompaction:
      return "Manual Compaction";
    case FlushReason::kWriteBufferManager:
      return "Write Buffer Manager";
    case FlushReason::kWriteBufferFull:
      return "Write Buffer Full";
    case FlushReason::kTest:
      return "Test";
    case FlushReason::kDeleteFiles:
      return "Delete Files";
    case FlushReason::kAutoCompaction:
      return "Auto Compaction";
    case FlushReason::kManualFlush:
      return "Manual Flush";
    case FlushReason::kErrorRecovery:
      return "Error Recovery";
    case FlushReason::kWalFull:
      return "WAL Full";
    default:
      return "Invalid";
  }
}

FlushJob::FlushJob(
    const std::string& dbname, ColumnFamilyData* cfd,
    const ImmutableDBOptions& db_options,
    const MutableCFOptions& mutable_cf_options, uint64_t max_memtable_id,
    const FileOptions& file_options, VersionSet* versions,
    InstrumentedMutex* db_mutex, std::atomic<bool>* shutting_down,
    std::vector<SequenceNumber> existing_snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    SnapshotChecker* snapshot_checker, JobContext* job_context,
    FlushReason flush_reason, LogBuffer* log_buffer, FSDirectory* db_directory,
    FSDirectory* output_file_directory, CompressionType output_compression,
    Statistics* stats, EventLogger* event_logger, bool measure_io_stats,
    const bool sync_output_directory, const bool write_manifest,
    Env::Priority thread_pri, const std::shared_ptr<IOTracer>& io_tracer,
    const SeqnoToTimeMapping& seqno_time_mapping, const std::string& db_id,
    const std::string& db_session_id, std::string full_history_ts_low,
    BlobFileCompletionCallback* blob_callback)
    : dbname_(dbname),
      db_id_(db_id),
      db_session_id_(db_session_id),
      cfd_(cfd),
      db_options_(db_options),
      mutable_cf_options_(mutable_cf_options),
      max_memtable_id_(max_memtable_id),
      file_options_(file_options),
      versions_(versions),
      db_mutex_(db_mutex),
      shutting_down_(shutting_down),
      existing_snapshots_(std::move(existing_snapshots)),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      snapshot_checker_(snapshot_checker),
      job_context_(job_context),
      flush_reason_(flush_reason),
      log_buffer_(log_buffer),
      db_directory_(db_directory),
      output_file_directory_(output_file_directory),
      output_compression_(output_compression),
      stats_(stats),
      event_logger_(event_logger),
      measure_io_stats_(measure_io_stats),
      sync_output_directory_(sync_output_directory),
      write_manifest_(write_manifest),
      edit_(nullptr),
      base_(nullptr),
      pick_memtable_called(false),
      thread_pri_(thread_pri),
      io_tracer_(io_tracer),
      clock_(db_options_.clock),
      full_history_ts_low_(std::move(full_history_ts_low)),
      blob_callback_(blob_callback),
      db_impl_seqno_time_mapping_(seqno_time_mapping) {
  // Update the thread status to indicate flush.
  ReportStartedFlush();
  TEST_SYNC_POINT("FlushJob::FlushJob()");
}

FlushJob::~FlushJob() { ThreadStatusUtil::ResetThreadStatus(); }

void FlushJob::ReportStartedFlush() {
  ThreadStatusUtil::SetColumnFamily(cfd_, cfd_->ioptions()->env,
                                    db_options_.enable_thread_tracking);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_FLUSH);
  ThreadStatusUtil::SetThreadOperationProperty(ThreadStatus::COMPACTION_JOB_ID,
                                               job_context_->job_id);
  IOSTATS_RESET(bytes_written);
}

void FlushJob::ReportFlushInputSize(const autovector<MemTable*>& mems) {
  uint64_t input_size = 0;
  for (auto* mem : mems) {
    input_size += mem->ApproximateMemoryUsage();
  }
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::FLUSH_BYTES_MEMTABLES, input_size);
}

void FlushJob::RecordFlushIOStats() {
  RecordTick(stats_, FLUSH_WRITE_BYTES, IOSTATS(bytes_written));
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}
void FlushJob::PickMemTable() {
  db_mutex_->AssertHeld();
  assert(!pick_memtable_called);
  pick_memtable_called = true;

  // Maximum "NextLogNumber" of the memtables to flush.
  // When mempurge feature is turned off, this variable is useless
  // because the memtables are implicitly sorted by increasing order of creation
  // time. Therefore mems_->back()->GetNextLogNumber() is already equal to
  // max_next_log_number. However when Mempurge is on, the memtables are no
  // longer sorted by increasing order of creation time. Therefore this variable
  // becomes necessary because mems_->back()->GetNextLogNumber() is no longer
  // necessarily equal to max_next_log_number.
  uint64_t max_next_log_number = 0;

  // Save the contents of the earliest memtable as a new Table
  cfd_->imm()->PickMemtablesToFlush(max_memtable_id_, &mems_,
                                    &max_next_log_number);
  if (mems_.empty()) {
    return;
  }

  ReportFlushInputSize(mems_);

  // entries mems are (implicitly) sorted in ascending order by their created
  // time. We will use the first memtable's `edit` to keep the meta info for
  // this flush.
  MemTable* m = mems_[0];
  edit_ = m->GetEdits();
  edit_->SetPrevLogNumber(0);
  // SetLogNumber(log_num) indicates logs with number smaller than log_num
  // will no longer be picked up for recovery.
  edit_->SetLogNumber(max_next_log_number);
  edit_->SetColumnFamily(cfd_->GetID());

  // path 0 for level 0 file.
  meta_.fd = FileDescriptor(versions_->NewFileNumber(), 0, 0);
  meta_.epoch_number = cfd_->NewEpochNumber();

  base_ = cfd_->current();
  base_->Ref();  // it is likely that we do not need this reference
}

Status FlushJob::Run(LogsWithPrepTracker* prep_tracker, FileMetaData* file_meta,
                     bool* switched_to_mempurge) {
  TEST_SYNC_POINT("FlushJob::Start");
  db_mutex_->AssertHeld();
  assert(pick_memtable_called);
  // Mempurge threshold can be dynamically changed.
  // For sake of consistency, mempurge_threshold is
  // saved locally to maintain consistency in each
  // FlushJob::Run call.
  double mempurge_threshold =
      mutable_cf_options_.experimental_mempurge_threshold;

  AutoThreadOperationStageUpdater stage_run(ThreadStatus::STAGE_FLUSH_RUN);
  if (mems_.empty()) {
    ROCKS_LOG_BUFFER(log_buffer_, "[%s] Nothing in memtable to flush",
                     cfd_->GetName().c_str());
    return Status::OK();
  }

  // I/O measurement variables
  PerfLevel prev_perf_level = PerfLevel::kEnableTime;
  uint64_t prev_write_nanos = 0;
  uint64_t prev_fsync_nanos = 0;
  uint64_t prev_range_sync_nanos = 0;
  uint64_t prev_prepare_write_nanos = 0;
  uint64_t prev_cpu_write_nanos = 0;
  uint64_t prev_cpu_read_nanos = 0;
  if (measure_io_stats_) {
    prev_perf_level = GetPerfLevel();
    SetPerfLevel(PerfLevel::kEnableTime);
    prev_write_nanos = IOSTATS(write_nanos);
    prev_fsync_nanos = IOSTATS(fsync_nanos);
    prev_range_sync_nanos = IOSTATS(range_sync_nanos);
    prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
    prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
    prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
  }
  Status mempurge_s = Status::NotFound("No MemPurge.");
  if ((mempurge_threshold > 0.0) &&
      (flush_reason_ == FlushReason::kWriteBufferFull) && (!mems_.empty()) &&
      MemPurgeDecider(mempurge_threshold) && !(db_options_.atomic_flush)) {
    cfd_->SetMempurgeUsed();
    mempurge_s = MemPurge();
    if (!mempurge_s.ok()) {
      // Mempurge is typically aborted when the output
      // bytes cannot be contained onto a single output memtable.
      if (mempurge_s.IsAborted()) {
        ROCKS_LOG_INFO(db_options_.info_log, "Mempurge process aborted: %s\n",
                       mempurge_s.ToString().c_str());
      } else {
        // However the mempurge process can also fail for
        // other reasons (eg: new_mem->Add() fails).
        ROCKS_LOG_WARN(db_options_.info_log, "Mempurge process failed: %s\n",
                       mempurge_s.ToString().c_str());
      }
    } else {
      if (switched_to_mempurge) {
        *switched_to_mempurge = true;
      } else {
        // The mempurge process was successful, but no switch_to_mempurge
        // pointer provided so no way to propagate the state of flush job.
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Mempurge process succeeded"
                       "but no 'switched_to_mempurge' ptr provided.\n");
      }
    }
  }
  Status s;
  if (mempurge_s.ok()) {
    base_->Unref();
    s = Status::OK();
  } else {
    // This will release and re-acquire the mutex.
    s = WriteLevel0Table();
  }

  if (s.ok() && cfd_->IsDropped()) {
    s = Status::ColumnFamilyDropped("Column family dropped during compaction");
  }
  if ((s.ok() || s.IsColumnFamilyDropped()) &&
      shutting_down_->load(std::memory_order_acquire)) {
    s = Status::ShutdownInProgress("Database shutdown");
  }

  if (!s.ok()) {
    cfd_->imm()->RollbackMemtableFlush(mems_, meta_.fd.GetNumber());
  } else if (write_manifest_) {
    TEST_SYNC_POINT("FlushJob::InstallResults");
    // Replace immutable memtable with the generated Table
    s = cfd_->imm()->TryInstallMemtableFlushResults(
        cfd_, mutable_cf_options_, mems_, prep_tracker, versions_, db_mutex_,
        meta_.fd.GetNumber(), &job_context_->memtables_to_free, db_directory_,
        log_buffer_, &committed_flush_jobs_info_,
        !(mempurge_s.ok()) /* write_edit : true if no mempurge happened (or if aborted),
                              but 'false' if mempurge successful: no new min log number
                              or new level 0 file path to write to manifest. */);
  }

  if (s.ok() && file_meta != nullptr) {
    *file_meta = meta_;
  }
  RecordFlushIOStats();

  // When measure_io_stats_ is true, the default 512 bytes is not enough.
  auto stream = event_logger_->LogToBuffer(log_buffer_, 1024);
  stream << "job" << job_context_->job_id << "event"
         << "flush_finished";
  stream << "output_compression"
         << CompressionTypeToString(output_compression_);
  stream << "lsm_state";
  stream.StartArray();
  auto vstorage = cfd_->current()->storage_info();
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    stream << vstorage->NumLevelFiles(level);
  }
  stream.EndArray();

  const auto& blob_files = vstorage->GetBlobFiles();
  if (!blob_files.empty()) {
    assert(blob_files.front());
    stream << "blob_file_head" << blob_files.front()->GetBlobFileNumber();

    assert(blob_files.back());
    stream << "blob_file_tail" << blob_files.back()->GetBlobFileNumber();
  }

  stream << "immutable_memtables" << cfd_->imm()->NumNotFlushed();

  if (measure_io_stats_) {
    if (prev_perf_level != PerfLevel::kEnableTime) {
      SetPerfLevel(prev_perf_level);
    }
    stream << "file_write_nanos" << (IOSTATS(write_nanos) - prev_write_nanos);
    stream << "file_range_sync_nanos"
           << (IOSTATS(range_sync_nanos) - prev_range_sync_nanos);
    stream << "file_fsync_nanos" << (IOSTATS(fsync_nanos) - prev_fsync_nanos);
    stream << "file_prepare_write_nanos"
           << (IOSTATS(prepare_write_nanos) - prev_prepare_write_nanos);
    stream << "file_cpu_write_nanos"
           << (IOSTATS(cpu_write_nanos) - prev_cpu_write_nanos);
    stream << "file_cpu_read_nanos"
           << (IOSTATS(cpu_read_nanos) - prev_cpu_read_nanos);
  }

  return s;
}

void FlushJob::Cancel() {
  db_mutex_->AssertHeld();
  assert(base_ != nullptr);
  base_->Unref();
}

Status FlushJob::MemPurge() {
  Status s;
  db_mutex_->AssertHeld();
  db_mutex_->Unlock();
  assert(!mems_.empty());

  // Measure purging time.
  const uint64_t start_micros = clock_->NowMicros();
  const uint64_t start_cpu_micros = clock_->CPUMicros();

  MemTable* new_mem = nullptr;
  // For performance/log investigation purposes:
  // look at how much useful payload we harvest in the new_mem.
  // This value is then printed to the DB log.
  double new_mem_capacity = 0.0;

  // Create two iterators, one for the memtable data (contains
  // info from puts + deletes), and one for the memtable
  // Range Tombstones (from DeleteRanges).
  ReadOptions ro;
  ro.total_order_seek = true;
  Arena arena;
  std::vector<InternalIterator*> memtables;
  std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
      range_del_iters;
  for (MemTable* m : mems_) {
    memtables.push_back(m->NewIterator(ro, &arena));
    auto* range_del_iter = m->NewRangeTombstoneIterator(
        ro, kMaxSequenceNumber, true /* immutable_memtable */);
    if (range_del_iter != nullptr) {
      range_del_iters.emplace_back(range_del_iter);
    }
  }

  assert(!memtables.empty());
  SequenceNumber first_seqno = kMaxSequenceNumber;
  SequenceNumber earliest_seqno = kMaxSequenceNumber;
  // Pick first and earliest seqno as min of all first_seqno
  // and earliest_seqno of the mempurged memtables.
  for (const auto& mem : mems_) {
    first_seqno = mem->GetFirstSequenceNumber() < first_seqno
                      ? mem->GetFirstSequenceNumber()
                      : first_seqno;
    earliest_seqno = mem->GetEarliestSequenceNumber() < earliest_seqno
                         ? mem->GetEarliestSequenceNumber()
                         : earliest_seqno;
  }

  ScopedArenaIterator iter(
      NewMergingIterator(&(cfd_->internal_comparator()), memtables.data(),
                         static_cast<int>(memtables.size()), &arena));

  auto* ioptions = cfd_->ioptions();

  // Place iterator at the First (meaning most recent) key node.
  iter->SeekToFirst();

  const std::string* const full_history_ts_low = &(cfd_->GetFullHistoryTsLow());
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&(cfd_->internal_comparator()),
                                       existing_snapshots_,
                                       full_history_ts_low));
  for (auto& rd_iter : range_del_iters) {
    range_del_agg->AddTombstones(std::move(rd_iter));
  }

  // If there is valid data in the memtable,
  // or at least range tombstones, copy over the info
  // to the new memtable.
  if (iter->Valid() || !range_del_agg->IsEmpty()) {
    // MaxSize is the size of a memtable.
    size_t maxSize = mutable_cf_options_.write_buffer_size;
    std::unique_ptr<CompactionFilter> compaction_filter;
    if (ioptions->compaction_filter_factory != nullptr &&
        ioptions->compaction_filter_factory->ShouldFilterTableFileCreation(
            TableFileCreationReason::kFlush)) {
      CompactionFilter::Context ctx;
      ctx.is_full_compaction = false;
      ctx.is_manual_compaction = false;
      ctx.column_family_id = cfd_->GetID();
      ctx.reason = TableFileCreationReason::kFlush;
      compaction_filter =
          ioptions->compaction_filter_factory->CreateCompactionFilter(ctx);
      if (compaction_filter != nullptr &&
          !compaction_filter->IgnoreSnapshots()) {
        s = Status::NotSupported(
            "CompactionFilter::IgnoreSnapshots() = false is not supported "
            "anymore.");
        return s;
      }
    }

    new_mem = new MemTable((cfd_->internal_comparator()), *(cfd_->ioptions()),
                           mutable_cf_options_, cfd_->write_buffer_mgr(),
                           earliest_seqno, cfd_->GetID());
    assert(new_mem != nullptr);

    Env* env = db_options_.env;
    assert(env);
    MergeHelper merge(
        env, (cfd_->internal_comparator()).user_comparator(),
        (ioptions->merge_operator).get(), compaction_filter.get(),
        ioptions->logger, true /* internal key corruption is not ok */,
        existing_snapshots_.empty() ? 0 : existing_snapshots_.back(),
        snapshot_checker_);
    assert(job_context_);
    SequenceNumber job_snapshot_seq = job_context_->GetJobSnapshotSequence();
    const std::atomic<bool> kManualCompactionCanceledFalse{false};
    CompactionIterator c_iter(
        iter.get(), (cfd_->internal_comparator()).user_comparator(), &merge,
        kMaxSequenceNumber, &existing_snapshots_,
        earliest_write_conflict_snapshot_, job_snapshot_seq, snapshot_checker_,
        env, ShouldReportDetailedTime(env, ioptions->stats),
        true /* internal key corruption is not ok */, range_del_agg.get(),
        nullptr, ioptions->allow_data_in_errors,
        ioptions->enforce_single_del_contracts,
        /*manual_compaction_canceled=*/kManualCompactionCanceledFalse,
        /*compaction=*/nullptr, compaction_filter.get(),
        /*hotspot_manager=*/nullptr,
        /*oldest_active_read_epoch=*/0,
        /*shutting_down=*/nullptr, ioptions->info_log, full_history_ts_low);

    // Set earliest sequence number in the new memtable
    // to be equal to the earliest sequence number of the
    // memtable being flushed (See later if there is a need
    // to update this number!).
    new_mem->SetEarliestSequenceNumber(earliest_seqno);
    // Likewise for first seq number.
    new_mem->SetFirstSequenceNumber(first_seqno);
    SequenceNumber new_first_seqno = kMaxSequenceNumber;

    c_iter.SeekToFirst();

    // Key transfer
    for (; c_iter.Valid(); c_iter.Next()) {
      const ParsedInternalKey ikey = c_iter.ikey();
      const Slice value = c_iter.value();
      new_first_seqno =
          ikey.sequence < new_first_seqno ? ikey.sequence : new_first_seqno;

      // Should we update "OldestKeyTime" ???? -> timestamp appear
      // to still be an "experimental" feature.
      s = new_mem->Add(
          ikey.sequence, ikey.type, ikey.user_key, value,
          nullptr,   // KV protection info set as nullptr since it
                     // should only be useful for the first add to
                     // the original memtable.
          false,     // : allow concurrent_memtable_writes_
                     // Not seen as necessary for now.
          nullptr,   // get_post_process_info(m) must be nullptr
                     // when concurrent_memtable_writes is switched off.
          nullptr);  // hint, only used when concurrent_memtable_writes_
                     // is switched on.
      if (!s.ok()) {
        break;
      }

      // If new_mem has size greater than maxSize,
      // then rollback to regular flush operation,
      // and destroy new_mem.
      if (new_mem->ApproximateMemoryUsage() > maxSize) {
        s = Status::Aborted("Mempurge filled more than one memtable.");
        new_mem_capacity = 1.0;
        break;
      }
    }

    // Check status and propagate
    // potential error status from c_iter
    if (!s.ok()) {
      c_iter.status().PermitUncheckedError();
    } else if (!c_iter.status().ok()) {
      s = c_iter.status();
    }

    // Range tombstone transfer.
    if (s.ok()) {
      auto range_del_it = range_del_agg->NewIterator();
      for (range_del_it->SeekToFirst(); range_del_it->Valid();
           range_del_it->Next()) {
        auto tombstone = range_del_it->Tombstone();
        new_first_seqno =
            tombstone.seq_ < new_first_seqno ? tombstone.seq_ : new_first_seqno;
        s = new_mem->Add(
            tombstone.seq_,        // Sequence number
            kTypeRangeDeletion,    // KV type
            tombstone.start_key_,  // Key is start key.
            tombstone.end_key_,    // Value is end key.
            nullptr,               // KV protection info set as nullptr since it
                                   // should only be useful for the first add to
                                   // the original memtable.
            false,                 // : allow concurrent_memtable_writes_
                                   // Not seen as necessary for now.
            nullptr,               // get_post_process_info(m) must be nullptr
                      // when concurrent_memtable_writes is switched off.
            nullptr);  // hint, only used when concurrent_memtable_writes_
                       // is switched on.

        if (!s.ok()) {
          break;
        }

        // If new_mem has size greater than maxSize,
        // then rollback to regular flush operation,
        // and destroy new_mem.
        if (new_mem->ApproximateMemoryUsage() > maxSize) {
          s = Status::Aborted(Slice("Mempurge filled more than one memtable."));
          new_mem_capacity = 1.0;
          break;
        }
      }
    }

    // If everything happened smoothly and new_mem contains valid data,
    // decide if it is flushed to storage or kept in the imm()
    // memtable list (memory).
    if (s.ok() && (new_first_seqno != kMaxSequenceNumber)) {
      // Rectify the first sequence number, which (unlike the earliest seq
      // number) needs to be present in the new memtable.
      new_mem->SetFirstSequenceNumber(new_first_seqno);

      // The new_mem is added to the list of immutable memtables
      // only if it filled at less than 100% capacity and isn't flagged
      // as in need of being flushed.
      if (new_mem->ApproximateMemoryUsage() < maxSize &&
          !(new_mem->ShouldFlushNow())) {
        // Construct fragmented memtable range tombstones without mutex
        new_mem->ConstructFragmentedRangeTombstones();
        db_mutex_->Lock();
        uint64_t new_mem_id = mems_[0]->GetID();

        new_mem->SetID(new_mem_id);
        new_mem->SetNextLogNumber(mems_[0]->GetNextLogNumber());

        // This addition will not trigger another flush, because
        // we do not call SchedulePendingFlush().
        cfd_->imm()->Add(new_mem, &job_context_->memtables_to_free);
        new_mem->Ref();
        // Piggyback FlushJobInfo on the first flushed memtable.
        db_mutex_->AssertHeld();
        meta_.fd.file_size = 0;
        mems_[0]->SetFlushJobInfo(GetFlushJobInfo());
        db_mutex_->Unlock();
      } else {
        s = Status::Aborted(Slice("Mempurge filled more than one memtable."));
        new_mem_capacity = 1.0;
        if (new_mem) {
          job_context_->memtables_to_free.push_back(new_mem);
        }
      }
    } else {
      // In this case, the newly allocated new_mem is empty.
      assert(new_mem != nullptr);
      job_context_->memtables_to_free.push_back(new_mem);
    }
  }

  // Reacquire the mutex for WriteLevel0 function.
  db_mutex_->Lock();

  // If mempurge successful, don't write input tables to level0,
  // but write any full output table to level0.
  if (s.ok()) {
    TEST_SYNC_POINT("DBImpl::FlushJob:MemPurgeSuccessful");
  } else {
    TEST_SYNC_POINT("DBImpl::FlushJob:MemPurgeUnsuccessful");
  }
  const uint64_t micros = clock_->NowMicros() - start_micros;
  const uint64_t cpu_micros = clock_->CPUMicros() - start_cpu_micros;
  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Mempurge lasted %" PRIu64
                 " microseconds, and %" PRIu64
                 " cpu "
                 "microseconds. Status is %s ok. Perc capacity: %f\n",
                 cfd_->GetName().c_str(), job_context_->job_id, micros,
                 cpu_micros, s.ok() ? "" : "not", new_mem_capacity);

  return s;
}

bool FlushJob::MemPurgeDecider(double threshold) {
  // Never trigger mempurge if threshold is not a strictly positive value.
  if (!(threshold > 0.0)) {
    return false;
  }
  if (threshold > (1.0 * mems_.size())) {
    return true;
  }
  // Payload and useful_payload (in bytes).
  // The useful payload ratio of a given MemTable
  // is estimated to be useful_payload/payload.
  uint64_t payload = 0, useful_payload = 0, entry_size = 0;

  // Local variables used repetitively inside the for-loop
  // when iterating over the sampled entries.
  Slice key_slice, value_slice;
  ParsedInternalKey res;
  SnapshotImpl min_snapshot;
  std::string vget;
  Status mget_s, parse_s;
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0, sqno = 0,
                 min_seqno_snapshot = 0;
  bool get_res, can_be_useful_payload, not_in_next_mems;

  // If estimated_useful_payload is > threshold,
  // then flush to storage, else MemPurge.
  double estimated_useful_payload = 0.0;
  // Cochran formula for determining sample size.
  // 95% confidence interval, 7% precision.
  //    n0 = (1.96*1.96)*0.25/(0.07*0.07) = 196.0
  double n0 = 196.0;
  ReadOptions ro;
  ro.total_order_seek = true;

  // Iterate over each memtable of the set.
  for (auto mem_iter = std::begin(mems_); mem_iter != std::end(mems_);
       mem_iter++) {
    MemTable* mt = *mem_iter;

    // Else sample from the table.
    uint64_t nentries = mt->num_entries();
    // Corrected Cochran formula for small populations
    // (converges to n0 for large populations).
    uint64_t target_sample_size =
        static_cast<uint64_t>(ceil(n0 / (1.0 + (n0 / nentries))));
    std::unordered_set<const char*> sentries = {};
    // Populate sample entries set.
    mt->UniqueRandomSample(target_sample_size, &sentries);

    // Estimate the garbage ratio by comparing if
    // each sample corresponds to a valid entry.
    for (const char* ss : sentries) {
      key_slice = GetLengthPrefixedSlice(ss);
      parse_s = ParseInternalKey(key_slice, &res, true /*log_err_key*/);
      if (!parse_s.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Memtable Decider: ParseInternalKey did not parse "
                       "key_slice %s successfully.",
                       key_slice.data());
      }

      // Size of the entry is "key size (+ value size if KV entry)"
      entry_size = key_slice.size();
      if (res.type == kTypeValue) {
        value_slice =
            GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
        entry_size += value_slice.size();
      }

      // Count entry bytes as payload.
      payload += entry_size;

      LookupKey lkey(res.user_key, kMaxSequenceNumber);

      // Paranoia: zero out these values just in case.
      max_covering_tombstone_seq = 0;
      sqno = 0;

      // Pick the oldest existing snapshot that is more recent
      // than the sequence number of the sampled entry.
      min_seqno_snapshot = kMaxSequenceNumber;
      for (SequenceNumber seq_num : existing_snapshots_) {
        if (seq_num > res.sequence && seq_num < min_seqno_snapshot) {
          min_seqno_snapshot = seq_num;
        }
      }
      min_snapshot.number_ = min_seqno_snapshot;
      ro.snapshot =
          min_seqno_snapshot < kMaxSequenceNumber ? &min_snapshot : nullptr;

      // Estimate if the sample entry is valid or not.
      get_res = mt->Get(lkey, &vget, /*columns=*/nullptr, /*timestamp=*/nullptr,
                        &mget_s, &merge_context, &max_covering_tombstone_seq,
                        &sqno, ro, true /* immutable_memtable */);
      if (!get_res) {
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "Memtable Get returned false when Get(sampled entry). "
            "Yet each sample entry should exist somewhere in the memtable, "
            "unrelated to whether it has been deleted or not.");
      }

      // TODO(bjlemaire): evaluate typeMerge.
      // This is where the sampled entry is estimated to be
      // garbage or not. Note that this is a garbage *estimation*
      // because we do not include certain items such as
      // CompactionFitlers triggered at flush, or if the same delete
      // has been inserted twice or more in the memtable.

      // Evaluate if the entry can be useful payload
      // Situation #1: entry is a KV entry, was found in the memtable mt
      //               and the sequence numbers match.
      can_be_useful_payload = (res.type == kTypeValue) && get_res &&
                              mget_s.ok() && (sqno == res.sequence);

      // Situation #2: entry is a delete entry, was found in the memtable mt
      //               (because gres==true) and no valid KV entry is found.
      //               (note: duplicate delete entries are also taken into
      //               account here, because the sequence number 'sqno'
      //               in memtable->Get(&sqno) operation is set to be equal
      //               to the most recent delete entry as well).
      can_be_useful_payload |=
          ((res.type == kTypeDeletion) || (res.type == kTypeSingleDeletion)) &&
          mget_s.IsNotFound() && get_res && (sqno == res.sequence);

      // If there is a chance that the entry is useful payload
      // Verify that the entry does not appear in the following memtables
      // (memtables with greater memtable ID/larger sequence numbers).
      if (can_be_useful_payload) {
        not_in_next_mems = true;
        for (auto next_mem_iter = mem_iter + 1;
             next_mem_iter != std::end(mems_); next_mem_iter++) {
          if ((*next_mem_iter)
                  ->Get(lkey, &vget, /*columns=*/nullptr, /*timestamp=*/nullptr,
                        &mget_s, &merge_context, &max_covering_tombstone_seq,
                        &sqno, ro, true /* immutable_memtable */)) {
            not_in_next_mems = false;
            break;
          }
        }
        if (not_in_next_mems) {
          useful_payload += entry_size;
        }
      }
    }
    if (payload > 0) {
      // We use the estimated useful payload ratio to
      // evaluate how many of the memtable bytes are useful bytes.
      estimated_useful_payload +=
          (mt->ApproximateMemoryUsage()) * (useful_payload * 1.0 / payload);

      ROCKS_LOG_INFO(db_options_.info_log,
                     "Mempurge sampling [CF %s] - found garbage ratio from "
                     "sampling: %f. Threshold is %f\n",
                     cfd_->GetName().c_str(),
                     (payload - useful_payload) * 1.0 / payload, threshold);
    } else {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Mempurge sampling: null payload measured, and collected "
                     "sample size is %zu\n.",
                     sentries.size());
    }
  }
  // We convert the total number of useful payload bytes
  // into the proportion of memtable necessary to store all these bytes.
  // We compare this proportion with the threshold value.
  return ((estimated_useful_payload / mutable_cf_options_.write_buffer_size) <
          threshold);
}

Status FlushJob::WriteLevel0Table() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_FLUSH_WRITE_L0);
  db_mutex_->AssertHeld();
  const uint64_t start_micros = clock_->NowMicros();
  const uint64_t start_cpu_micros = clock_->CPUMicros();
  Status s;

  SequenceNumber smallest_seqno = mems_.front()->GetEarliestSequenceNumber();
  if (!db_impl_seqno_time_mapping_.Empty()) {
    // make a local copy, as the seqno_time_mapping from db_impl is not thread
    // safe, which will be used while not holding the db_mutex.
    seqno_to_time_mapping_ = db_impl_seqno_time_mapping_.Copy(smallest_seqno);
  }

  std::vector<BlobFileAddition> blob_file_additions;

  {
    auto write_hint = cfd_->CalculateSSTWriteHint(0);
    Env::IOPriority io_priority = GetRateLimiterPriorityForWrite();
    db_mutex_->Unlock();
    if (log_buffer_) {
      log_buffer_->FlushBufferToLog();
    }
    // memtables and range_del_iters store internal iterators over each data
    // memtable and its associated range deletion memtable, respectively, at
    // corresponding indexes.
    std::vector<InternalIterator*> memtables;
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
        range_del_iters;
    ReadOptions ro;
    ro.total_order_seek = true;
    Arena arena;
    uint64_t total_num_entries = 0, total_num_deletes = 0;
    uint64_t total_data_size = 0;
    size_t total_memory_usage = 0;
    // Used for testing:
    uint64_t mems_size = mems_.size();
    (void)mems_size;  // avoids unused variable error when
                      // TEST_SYNC_POINT_CALLBACK not used.
    TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table:num_memtables",
                             &mems_size);
    assert(job_context_);
    for (MemTable* m : mems_) {
      ROCKS_LOG_INFO(
          db_options_.info_log,
          "[%s] [JOB %d] Flushing memtable with next log file: %" PRIu64 "\n",
          cfd_->GetName().c_str(), job_context_->job_id, m->GetNextLogNumber());
      memtables.push_back(m->NewIterator(ro, &arena));
      auto* range_del_iter = m->NewRangeTombstoneIterator(
          ro, kMaxSequenceNumber, true /* immutable_memtable */);
      if (range_del_iter != nullptr) {
        range_del_iters.emplace_back(range_del_iter);
      }
      total_num_entries += m->num_entries();
      total_num_deletes += m->num_deletes();
      total_data_size += m->get_data_size();
      total_memory_usage += m->ApproximateMemoryUsage();
    }

    event_logger_->Log() << "job" << job_context_->job_id << "event"
                         << "flush_started"
                         << "num_memtables" << mems_.size() << "num_entries"
                         << total_num_entries << "num_deletes"
                         << total_num_deletes << "total_data_size"
                         << total_data_size << "memory_usage"
                         << total_memory_usage << "flush_reason"
                         << GetFlushReasonString(flush_reason_);

    {
      ScopedArenaIterator iter(
          NewMergingIterator(&cfd_->internal_comparator(), memtables.data(),
                             static_cast<int>(memtables.size()), &arena));
      ROCKS_LOG_INFO(db_options_.info_log,
                     "[%s] [JOB %d] Level-0 flush table #%" PRIu64 ": started",
                     cfd_->GetName().c_str(), job_context_->job_id,
                     meta_.fd.GetNumber());

      TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table:output_compression",
                               &output_compression_);
      int64_t _current_time = 0;
      auto status = clock_->GetCurrentTime(&_current_time);
      // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
      if (!status.ok()) {
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "Failed to get current time to populate creation_time property. "
            "Status: %s",
            status.ToString().c_str());
      }
      const uint64_t current_time = static_cast<uint64_t>(_current_time);

      uint64_t oldest_key_time = mems_.front()->ApproximateOldestKeyTime();

      // It's not clear whether oldest_key_time is always available. In case
      // it is not available, use current_time.
      uint64_t oldest_ancester_time = std::min(current_time, oldest_key_time);

      TEST_SYNC_POINT_CALLBACK(
          "FlushJob::WriteLevel0Table:oldest_ancester_time",
          &oldest_ancester_time);
      meta_.oldest_ancester_time = oldest_ancester_time;
      meta_.file_creation_time = current_time;

      uint64_t num_input_entries = 0;
      uint64_t memtable_payload_bytes = 0;
      uint64_t memtable_garbage_bytes = 0;
      IOStatus io_s;
      // 记录本次 flush 产生的所有 L0 文件元信息。
      //
      // 普通 flush 通常只产生一个 SST 文件，原有逻辑只需要使用 meta_。
      // Delta partitioned flush 可能会按 Delta L0 partition 边界拆成多个 SST，
      // 因此需要用 output_metas 收集每个输出文件，最后统一写入 VersionEdit。
      std::vector<FileMetaData> output_metas;

      const std::string* const full_history_ts_low =
          (full_history_ts_low_.empty()) ? nullptr : &full_history_ts_low_;
      const SequenceNumber job_snapshot_seq =
          job_context_->GetJobSnapshotSequence();
      // 判断本次 flush 是否启用 Delta partition-aware flush。
      //
      // 启用条件：
      // 1. 当前 Column Family 是 Delta Column Family；
      // 2. Delta L0 partition directory 已经初始化；
      // 3. 本次 flush 不包含 range deletion iterator。
      //
      // range_del_iters 为空才启用，是因为 partitioned flush 会把一个 memtable
      // 按 partition 拆成多个 SST；range deletion 的边界和可见性处理更复杂，
      // 当前仍走普通 flush 路径，避免错误拆分 range deletion。
      const bool use_delta_partitioned_flush =
          cfd_->IsDeltaColumnFamily() && cfd_->delta_l0_partition_directory() &&
          range_del_iters.empty();
      if (use_delta_partitioned_flush) {
        // 用 DeltaPartitionedFlushIterator 包装原始 memtable iterator。
        //
        // 该 iterator 会按 Delta L0 partition 边界控制 Valid()：
        // 当前 partition 内的数据返回 valid；
        // 一旦遇到下一个 partition 的 key，就停止当前 BuildTable，
        // 让外层循环创建下一个 SST 文件。
        DeltaPartitionedFlushIterator partitioned_iter(
            iter.get(), cfd_->delta_l0_partition_directory());

        // 汇总所有 partition 输出文件的输入条数和 memtable 字节统计。
        // 最终会写回 num_input_entries / memtable_payload_bytes /
        // memtable_garbage_bytes，以保持后续校验和统计逻辑不变。
        uint64_t total_input_entries = 0;
        uint64_t total_payload_bytes = 0;
        uint64_t total_garbage_bytes = 0;

        // 第一个输出文件沿用 FlushJob 原本分配好的 meta_ 和 file number。
        // 如果同一次 flush 需要生成更多 partition 文件，后续文件再申请新 file number。
        bool first_output = true;

        // 循环构建多个 SST 文件。
        // 每次 BuildTable 只消费 partitioned_iter 当前 partition 范围内的数据。
        // 如果底层 iterator 仍有未处理数据，则继续下一轮，为下一个 partition 建文件。
        while (s.ok()) {
          FileMetaData file_meta;
          if (first_output) {
            file_meta = meta_;
          } else {
            file_meta.fd = FileDescriptor(versions_->NewFileNumber(), 0, 0);
            file_meta.epoch_number = cfd_->NewEpochNumber();
          }
          file_meta.oldest_ancester_time = oldest_ancester_time;
          file_meta.file_creation_time = current_time;

          TableProperties file_table_properties;
          uint64_t file_input_entries = 0;
          uint64_t file_payload_bytes = 0;
          uint64_t file_garbage_bytes = 0;

          // 为当前 partition 输出文件构造 TableBuilderOptions。
          // level 固定为 0，因为 flush 产生的文件总是进入 L0。
          // file_meta.fd.GetNumber() 对应当前输出 SST 的文件号。
          TableBuilderOptions tboptions(
              *cfd_->ioptions(), mutable_cf_options_,
              cfd_->internal_comparator(),
              cfd_->int_tbl_prop_collector_factories(), output_compression_,
              mutable_cf_options_.compression_opts, cfd_->GetID(),
              cfd_->GetName(), 0 /* level */, false /* is_bottommost */,
              TableFileCreationReason::kFlush, oldest_key_time, current_time,
              db_id_, db_session_id_, 0 /* target_file_size */,
              file_meta.fd.GetNumber());

          // 构建当前 partition 对应的 L0 SST 文件。
          //
          // 这里传入的是 partitioned_iter，而不是原始 iter。
          // 因此 BuildTable 只会读取当前 Delta L0 partition 范围内的数据；
          // 当 iterator 跨到下一个 partition 时，当前 BuildTable 会自然结束。
          s = BuildTable(
              dbname_, versions_, db_options_, tboptions, file_options_,
              cfd_->table_cache(), &partitioned_iter,
              {} /* range_del_iters */, &file_meta, &blob_file_additions,
              existing_snapshots_, earliest_write_conflict_snapshot_,
              job_snapshot_seq, snapshot_checker_,
              mutable_cf_options_.paranoid_file_checks, cfd_->internal_stats(),
              &io_s, io_tracer_, BlobFileCreationReason::kFlush,
              seqno_to_time_mapping_, event_logger_, job_context_->job_id,
              io_priority, &file_table_properties, write_hint,
              full_history_ts_low, blob_callback_, base_, &file_input_entries,
              &file_payload_bytes, &file_garbage_bytes);
          assert(!s.ok() || io_s.ok());
          io_s.PermitUncheckedError();
          if (!s.ok()) {
            break;
          }
          
          // 汇总当前输出文件的条目数和 memtable 字节统计。
          total_input_entries += file_input_entries;
          total_payload_bytes += file_payload_bytes;
          total_garbage_bytes += file_garbage_bytes;

          if (file_meta.fd.GetFileSize() > 0) {
            // 为当前输出 L0 文件填充 Delta L0 tableid 范围元信息。
            // 该函数会根据文件 smallest/largest key 解析 tableid_min/tableid_max。
            MaybePopulateDeltaL0FileMeta(cfd_->GetName(), 0, &file_meta);
            if (file_meta.delta_l0_meta.valid) {
              // 将当前 SST 绑定到 partitioned_iter 当前处理的 Delta L0 partition。
              //
              // 这样文件元信息不仅包含 tableid_min/tableid_max，
              // 还包含 partition_id 和 partition_generation，
              // 后续读路径剪枝、Intra-L0 compaction、reopen 恢复都可以使用该身份信息。
              file_meta.delta_l0_meta.partition_id =
                  partitioned_iter.current_partition().partition_id;
              file_meta.delta_l0_meta.partition_generation =
                  partitioned_iter.current_partition().generation;
            }
            // 收集当前输出文件，最后统一 AddFile 到 VersionEdit。
            output_metas.emplace_back(file_meta);
            if (first_output) {
              // 兼容原有 flush 逻辑：
              // meta_ 和 table_properties_ 仍表示第一个输出文件，
              // 供后续日志、统计以及已有流程使用。
              meta_ = file_meta;
              table_properties_ = file_table_properties;
            }
            first_output = false;
          }

          // 如果底层 iterator 已经没有更多数据，说明所有 partition 都处理完毕。
          // 否则继续 while 循环，为下一个 partition 创建新的 SST。
          if (!partitioned_iter.HasPendingData()) {
            break;
          }
        }

        // 将多个 partition 输出文件的累计统计写回原有变量，
        // 保证后续 memtable 条数校验和统计记录仍然覆盖整个 flush。
        num_input_entries = total_input_entries;
        memtable_payload_bytes = total_payload_bytes;
        memtable_garbage_bytes = total_garbage_bytes;
      } else {
        // 普通 flush 路径。
        //
        // 非 Delta Column Family、没有 Delta L0 partition directory、
        // 或包含 range deletion 的 flush，仍然使用原始 iterator 一次性构建单个 L0 SST。
        TableBuilderOptions tboptions(
            *cfd_->ioptions(), mutable_cf_options_,
            cfd_->internal_comparator(),
            cfd_->int_tbl_prop_collector_factories(), output_compression_,
            mutable_cf_options_.compression_opts, cfd_->GetID(),
            cfd_->GetName(), 0 /* level */, false /* is_bottommost */,
            TableFileCreationReason::kFlush, oldest_key_time, current_time,
            db_id_, db_session_id_, 0 /* target_file_size */,
            meta_.fd.GetNumber());
        s = BuildTable(
            dbname_, versions_, db_options_, tboptions, file_options_,
            cfd_->table_cache(), iter.get(), std::move(range_del_iters), &meta_,
            &blob_file_additions, existing_snapshots_,
            earliest_write_conflict_snapshot_, job_snapshot_seq,
            snapshot_checker_, mutable_cf_options_.paranoid_file_checks,
            cfd_->internal_stats(), &io_s, io_tracer_,
            BlobFileCreationReason::kFlush, seqno_to_time_mapping_,
            event_logger_, job_context_->job_id, io_priority,
            &table_properties_, write_hint, full_history_ts_low, blob_callback_,
            base_, &num_input_entries, &memtable_payload_bytes,
            &memtable_garbage_bytes);
          assert(!s.ok() || io_s.ok());
          io_s.PermitUncheckedError();
          if (s.ok() && meta_.fd.GetFileSize() > 0) {
            output_metas.emplace_back(meta_);
          }
      }
      // TODO: Cleanup io_status in BuildTable and table builders
      if (num_input_entries != total_num_entries && s.ok()) {
        std::string msg = "Expected " + std::to_string(total_num_entries) +
                          " entries in memtables, but read " +
                          std::to_string(num_input_entries);
        ROCKS_LOG_WARN(db_options_.info_log, "[%s] [JOB %d] Level-0 flush %s",
                       cfd_->GetName().c_str(), job_context_->job_id,
                       msg.c_str());
        if (db_options_.flush_verify_memtable_count) {
          s = Status::Corruption(msg);
        }
      }
      if (s.ok()) {
        TEST_SYNC_POINT("DBImpl::FlushJob:Flush");
        RecordTick(stats_, MEMTABLE_PAYLOAD_BYTES_AT_FLUSH,
                   memtable_payload_bytes);
        RecordTick(stats_, MEMTABLE_GARBAGE_BYTES_AT_FLUSH,
                   memtable_garbage_bytes);
      }
      LogFlush(db_options_.info_log);

      if (s.ok() && !output_metas.empty()) {
        // 将本次 flush 产生的所有 L0 文件写入 VersionEdit。
        //
        // 普通 flush 通常只有一个 output_meta；
        // Delta partitioned flush 可能有多个 output_meta，
        // 每个文件都需要单独 AddFile，并携带各自的 delta_l0_meta。
        for (const auto& output_meta : output_metas) {
          edit_->AddFile(
              0 /* level */, output_meta.fd.GetNumber(),
              output_meta.fd.GetPathId(), output_meta.fd.GetFileSize(),
              output_meta.smallest, output_meta.largest,
              output_meta.fd.smallest_seqno, output_meta.fd.largest_seqno,
              output_meta.marked_for_compaction, output_meta.temperature,
              output_meta.oldest_blob_file_number,
              output_meta.oldest_ancester_time, output_meta.file_creation_time,
              output_meta.epoch_number, output_meta.file_checksum,
              output_meta.file_checksum_func_name, output_meta.unique_id,
              output_meta.compensated_range_deletion_size,
              output_meta.delta_l0_meta);
        }
        if (use_delta_partitioned_flush) {
          edit_->SetDeltaL0PartitionSnapshot(
              cfd_->delta_l0_partition_directory()->EncodeSnapshot());
        }
      }
    }
    ROCKS_LOG_BUFFER(log_buffer_,
                     "[%s] [JOB %d] Level-0 flush table #%" PRIu64 ": %" PRIu64
                     " bytes %s"
                     "%s",
                     cfd_->GetName().c_str(), job_context_->job_id,
                     meta_.fd.GetNumber(), meta_.fd.GetFileSize(),
                     s.ToString().c_str(),
                     meta_.marked_for_compaction ? " (needs compaction)" : "");

    if (s.ok() && output_file_directory_ != nullptr && sync_output_directory_) {
      s = output_file_directory_->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
    }
    TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table", &mems_);
    db_mutex_->Lock();
  }
  base_->Unref();

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  const bool has_output = meta_.fd.GetFileSize() > 0;

  if (s.ok() && has_output) {
    TEST_SYNC_POINT("DBImpl::FlushJob:SSTFileCreated");
    edit_->SetBlobFileAdditions(std::move(blob_file_additions));
  }
  // Piggyback FlushJobInfo on the first first flushed memtable.
  mems_[0]->SetFlushJobInfo(GetFlushJobInfo());

  // Note that here we treat flush as level 0 compaction in internal stats
  InternalStats::CompactionStats stats(CompactionReason::kFlush, 1);
  const uint64_t micros = clock_->NowMicros() - start_micros;
  const uint64_t cpu_micros = clock_->CPUMicros() - start_cpu_micros;
  stats.micros = micros;
  stats.cpu_micros = cpu_micros;

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Flush lasted %" PRIu64
                 " microseconds, and %" PRIu64 " cpu microseconds.\n",
                 cfd_->GetName().c_str(), job_context_->job_id, micros,
                 cpu_micros);

  if (has_output) {
    stats.bytes_written = meta_.fd.GetFileSize();
    stats.num_output_files = 1;
  }

  const auto& blobs = edit_->GetBlobFileAdditions();
  for (const auto& blob : blobs) {
    stats.bytes_written_blob += blob.GetTotalBlobBytes();
  }

  stats.num_output_files_blob = static_cast<int>(blobs.size());

  RecordTimeToHistogram(stats_, FLUSH_TIME, stats.micros);
  cfd_->internal_stats()->AddCompactionStats(0 /* level */, thread_pri_, stats);
  cfd_->internal_stats()->AddCFStats(
      InternalStats::BYTES_FLUSHED,
      stats.bytes_written + stats.bytes_written_blob);
  RecordFlushIOStats();

  return s;
}

Env::IOPriority FlushJob::GetRateLimiterPriorityForWrite() {
  if (versions_ && versions_->GetColumnFamilySet() &&
      versions_->GetColumnFamilySet()->write_controller()) {
    WriteController* write_controller =
        versions_->GetColumnFamilySet()->write_controller();
    if (write_controller->IsStopped() || write_controller->NeedsDelay()) {
      return Env::IO_USER;
    }
  }

  return Env::IO_HIGH;
}

std::unique_ptr<FlushJobInfo> FlushJob::GetFlushJobInfo() const {
  db_mutex_->AssertHeld();
  std::unique_ptr<FlushJobInfo> info(new FlushJobInfo{});
  info->cf_id = cfd_->GetID();
  info->cf_name = cfd_->GetName();

  const uint64_t file_number = meta_.fd.GetNumber();
  info->file_path =
      MakeTableFileName(cfd_->ioptions()->cf_paths[0].path, file_number);
  info->file_number = file_number;
  info->oldest_blob_file_number = meta_.oldest_blob_file_number;
  info->thread_id = db_options_.env->GetThreadID();
  info->job_id = job_context_->job_id;
  info->smallest_seqno = meta_.fd.smallest_seqno;
  info->largest_seqno = meta_.fd.largest_seqno;
  info->table_properties = table_properties_;
  info->flush_reason = flush_reason_;
  info->blob_compression_type = mutable_cf_options_.blob_compression_type;

  // Update BlobFilesInfo.
  for (const auto& blob_file : edit_->GetBlobFileAdditions()) {
    BlobFileAdditionInfo blob_file_addition_info(
        BlobFileName(cfd_->ioptions()->cf_paths.front().path,
                     blob_file.GetBlobFileNumber()) /*blob_file_path*/,
        blob_file.GetBlobFileNumber(), blob_file.GetTotalBlobCount(),
        blob_file.GetTotalBlobBytes());
    info->blob_file_addition_infos.emplace_back(
        std::move(blob_file_addition_info));
  }
  return info;
}

}  // namespace ROCKSDB_NAMESPACE
