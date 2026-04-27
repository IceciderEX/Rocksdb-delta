#include "delta/hotspot_manager.h"

#include <algorithm>
#include <cinttypes>
#include <cstring>
#include <unordered_map>

#include "delta/diag_log.h"
#include "db/delta_l0.h"
#include "db/dbformat.h"
#include "logging/logging.h"
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr char kGDCTLogMagic[8] = {'G', 'D', 'C', 'T', 'V', '2', '\r', '\n'};
constexpr size_t kGDCTLogMagicSize = sizeof(kGDCTLogMagic);
constexpr size_t kGDCTWalPayloadSize = 1 + sizeof(uint64_t) * 5;
constexpr size_t kGDCTWalRecordSize = kGDCTWalPayloadSize + sizeof(uint32_t);

void EncodeWalRecord(const GDCTDeleteRecord& record, std::string* out) {
  out->clear();
  out->reserve(kGDCTWalRecordSize);
  out->push_back(static_cast<char>(record.record_type));

  char buffer[sizeof(uint64_t)] = {};
  EncodeFixed64(buffer, record.key.dbid);
  out->append(buffer, sizeof(buffer));
  EncodeFixed64(buffer, record.key.tableid);
  out->append(buffer, sizeof(buffer));
  EncodeFixed64(buffer, record.key.cuid);
  out->append(buffer, sizeof(buffer));
  EncodeFixed64(buffer, record.cutoff_seq);
  out->append(buffer, sizeof(buffer));
  EncodeFixed64(buffer, record.publish_epoch);
  out->append(buffer, sizeof(buffer));

  const uint32_t checksum = crc32c::Mask(crc32c::Value(out->data(), out->size()));
  char checksum_buffer[sizeof(uint32_t)] = {};
  EncodeFixed32(checksum_buffer, checksum);
  out->append(checksum_buffer, sizeof(checksum_buffer));
}

bool DecodeWalRecord(const Slice& raw_record, GDCTDeleteRecord* record) {
  if (raw_record.size() != kGDCTWalRecordSize) {
    return false;
  }

  const uint32_t expected_checksum =
      DecodeFixed32(raw_record.data() + kGDCTWalPayloadSize);
  const uint32_t actual_checksum = crc32c::Mask(
      crc32c::Value(raw_record.data(), kGDCTWalPayloadSize));
  if (expected_checksum != actual_checksum) {
    return false;
  }

  record->record_type =
      static_cast<GDCTRecordType>(static_cast<uint8_t>(raw_record.data()[0]));
  record->key.dbid = DecodeFixed64(raw_record.data() + 1);
  record->key.tableid = DecodeFixed64(raw_record.data() + 1 + sizeof(uint64_t));
  record->key.cuid = DecodeFixed64(raw_record.data() + 1 + sizeof(uint64_t) * 2);
  record->cutoff_seq =
      DecodeFixed64(raw_record.data() + 1 + sizeof(uint64_t) * 3);
  record->publish_epoch =
      DecodeFixed64(raw_record.data() + 1 + sizeof(uint64_t) * 4);
  return record->record_type == GDCTRecordType::kMergeCommit ||
         record->record_type == GDCTRecordType::kPrunedCutoff;
}

struct ThreadReadEpochPinState {
  HotspotManagerReadEpochSlot* slot = nullptr;
  std::vector<uint64_t> pinned_epochs;
};

thread_local std::unordered_map<const HotspotManager*, ThreadReadEpochPinState>
    tls_read_epoch_pins;

}  // namespace

HotspotManager::HotspotManager(const ImmutableOptions& options,
                               const std::string& data_dir,
                               const std::string& delta_cf_name,
                               const InternalKeyComparator* internal_comparator)
    : options_(&options),
      data_dir_(data_dir),
      delta_cf_name_(delta_cf_name),
      internal_comparator_(internal_comparator),
      delta_options_(options.delta_options),
      delete_table_(delta_options_.sharding_count == 0 ? 1
                                                       : delta_options_.sharding_count) {
  options_->env->CreateDirIfMissing(data_dir_);
  DiagLogOpen((data_dir_ + "/delta_diag.log").c_str());
  LifecycleLogOpen((data_dir_ + "/hotsst_lifecycle.log").c_str());
  DiagLogf("[INFO] GDCT-only HotspotManager initialized, shards=%u\n",
           delta_options_.sharding_count == 0 ? 1 : delta_options_.sharding_count);
  RecoverGDCT();
  oldest_active_read_epoch_.store(CurrentEpoch(), std::memory_order_release);
  InitGDCTLog();
}

uint64_t HotspotManager::AllocateEpoch() {
  const uint64_t epoch =
      current_epoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
  if (oldest_active_read_epoch_.load(std::memory_order_acquire) == 0) {
    oldest_active_read_epoch_.store(CurrentEpoch(), std::memory_order_release);
  }
  return epoch;
}

uint64_t HotspotManager::FreezeReadEpoch() const {
  while (true) {
    const uint64_t barrier_before = GetPublishBarrier();
    if ((barrier_before & 1U) != 0) {
      continue;
    }

    const uint64_t epoch = CurrentEpoch();
    const uint64_t barrier_after = GetPublishBarrier();
    if (barrier_before == barrier_after) {
      return epoch;
    }
  }
}

uint64_t HotspotManager::AcquireReadEpoch() {
  const uint64_t epoch = FreezeReadEpoch();
  AcquireReadEpoch(epoch);
  return epoch;
}

void HotspotManager::AcquireReadEpoch(uint64_t epoch) {
  ThreadReadEpochPinState& pin_state = tls_read_epoch_pins[this];
  if (pin_state.slot == nullptr) {
    pin_state.slot = RegisterReadEpochSlot();
  }

  pin_state.pinned_epochs.push_back(epoch);
  const uint64_t min_epoch = *std::min_element(pin_state.pinned_epochs.begin(),
                                               pin_state.pinned_epochs.end());
  pin_state.slot->pinned_epoch.store(min_epoch, std::memory_order_release);

  uint64_t oldest_epoch =
      oldest_active_read_epoch_.load(std::memory_order_acquire);
  while ((oldest_epoch == 0 || min_epoch < oldest_epoch) &&
         !oldest_active_read_epoch_.compare_exchange_weak(
             oldest_epoch, min_epoch, std::memory_order_acq_rel,
             std::memory_order_acquire)) {
  }
}

void HotspotManager::ReleaseReadEpoch(uint64_t epoch) {
  auto pin_it = tls_read_epoch_pins.find(this);
  if (pin_it == tls_read_epoch_pins.end()) {
    return;
  }

  ThreadReadEpochPinState& pin_state = pin_it->second;
  auto epoch_it = std::find(pin_state.pinned_epochs.begin(),
                            pin_state.pinned_epochs.end(), epoch);
  if (epoch_it == pin_state.pinned_epochs.end()) {
    return;
  }
  pin_state.pinned_epochs.erase(epoch_it);

  if (pin_state.pinned_epochs.empty()) {
    pin_state.slot->pinned_epoch.store(0, std::memory_order_release);
  } else {
    const uint64_t min_epoch =
        *std::min_element(pin_state.pinned_epochs.begin(),
                          pin_state.pinned_epochs.end());
    pin_state.slot->pinned_epoch.store(min_epoch, std::memory_order_release);
  }
}

void HotspotManager::PublishEpoch(uint64_t epoch) {
  publish_barrier_.fetch_add(1, std::memory_order_acq_rel);
  const uint64_t current_published = CurrentEpoch();
  if (epoch > current_published) {
    published_epoch_.store(epoch, std::memory_order_release);
  }
  publish_barrier_.fetch_add(1, std::memory_order_acq_rel);

  if (oldest_active_read_epoch_.load(std::memory_order_acquire) == 0) {
    oldest_active_read_epoch_.store(CurrentEpoch(), std::memory_order_release);
  }
}

uint64_t HotspotManager::GetOldestActiveReadEpoch() const {
  return RefreshOldestActiveReadEpoch();
}

HotspotManagerReadEpochSlot* HotspotManager::RegisterReadEpochSlot() {
  std::lock_guard<std::mutex> lock(read_epoch_slots_mutex_);
  read_epoch_slots_.emplace_back(new HotspotManagerReadEpochSlot());
  return read_epoch_slots_.back().get();
}

uint64_t HotspotManager::RefreshOldestActiveReadEpoch() const {
  const uint64_t current_epoch = CurrentEpoch();
  uint64_t oldest_epoch = current_epoch;
  bool has_active_reader = false;

  std::lock_guard<std::mutex> lock(read_epoch_slots_mutex_);
  for (const auto& slot : read_epoch_slots_) {
    const uint64_t pinned_epoch =
        slot->pinned_epoch.load(std::memory_order_acquire);
    if (pinned_epoch == 0) {
      continue;
    }
    has_active_reader = true;
    if (pinned_epoch < oldest_epoch) {
      oldest_epoch = pinned_epoch;
    }
  }

  if (!has_active_reader) {
    oldest_epoch = current_epoch;
  }
  oldest_active_read_epoch_.store(oldest_epoch, std::memory_order_release);
  return oldest_epoch;
}

bool HotspotManager::ExtractGDCTKey(const Slice& key, GDCTKey* gdct_key) const {
  if (gdct_key == nullptr) {
    return false;
  }

  uint32_t dbid = 0;
  uint32_t relid = 0;
  uint64_t cuid = 0;
  if (!TryParseDeltaBinaryKeyPrefix(key, &dbid, &relid, &cuid)) {
    return false;
  }

  gdct_key->dbid = dbid;
  gdct_key->tableid = relid;
  gdct_key->cuid = cuid;
  return true;
}

uint64_t HotspotManager::ExtractCUID(const Slice& key) const {
  GDCTKey gdct_key;
  if (!ExtractGDCTKey(key, &gdct_key)) {
    return 0;
  }
  return gdct_key.cuid;
}

bool HotspotManager::ShouldHideForRead(const Slice& key, SequenceNumber put_seq,
                                       uint64_t read_epoch) const {
  GDCTKey gdct_key;
  if (!ExtractGDCTKey(key, &gdct_key)) {
    return false;
  }
  return delete_table_.ShouldHideForRead(gdct_key, put_seq, read_epoch);
}

bool HotspotManager::ShouldDropDuringCompaction(
    const Slice& key, SequenceNumber put_seq,
    uint64_t oldest_active_read_epoch) const {
  GDCTKey gdct_key;
  if (!ExtractGDCTKey(key, &gdct_key)) {
    return false;
  }
  return delete_table_.ShouldDropDuringCompaction(gdct_key, put_seq,
                                                  oldest_active_read_epoch);
}

bool HotspotManager::TrackPhysicalUnit(const Slice& key, uint64_t phys_id) {
  GDCTKey gdct_key;
  if (!ExtractGDCTKey(key, &gdct_key)) {
    return false;
  }
  return delete_table_.TrackPhysicalUnit(gdct_key, phys_id);
}

Status HotspotManager::InterceptDelete(const Slice& key, SequenceNumber seq,
                                       bool sync) {
  (void)sync;
  GDCTKey gdct_key;
  if (!ExtractGDCTKey(key, &gdct_key)) {
    return Status::NotSupported("GDCT key not found");
  }

  const uint64_t publish_epoch = AllocateEpoch();

  GDCTDeleteRecord record;
  record.record_type = GDCTRecordType::kMergeCommit;
  record.key = gdct_key;
  record.cutoff_seq = seq;
  record.publish_epoch = publish_epoch;
  Status s = PersistDelete(record, true /* sync */);
  if (!s.ok()) {
    return s;
  }

  bool newly_published = false;
  delete_table_.PublishRecord(gdct_key, seq, publish_epoch, &newly_published);
  PublishEpoch(publish_epoch);
  return Status::OK();
}

Status HotspotManager::InitGDCTLog() {
  std::lock_guard<std::mutex> lock(gdct_log_mutex_);
  return InitGDCTLogLocked();
}

Status HotspotManager::InitGDCTLogLocked() {
  if (gdct_log_writer_) {
    return Status::OK();
  }

  std::string log_dir = data_dir_ + "/hot_shared_";
  options_->env->CreateDirIfMissing(log_dir);
  std::string log_path = log_dir + "/gdct.log";

  EnvOptions env_options;
  Status s =
      options_->env->ReopenWritableFile(log_path, &gdct_log_writer_, env_options);
  if (s.IsNotSupported()) {
    s = options_->env->NewWritableFile(log_path, &gdct_log_writer_, env_options);
  }
  if (!s.ok()) {
    ROCKS_LOG_ERROR(options_->info_log,
                    "[HotspotManager] Failed to open GDCT log file: %s",
                    s.ToString().c_str());
    return s;
  }

  uint64_t file_size = 0;
  s = options_->env->GetFileSize(log_path, &file_size);
  if (!s.ok()) {
    return s;
  }
  if (file_size == 0) {
    s = gdct_log_writer_->Append(Slice(kGDCTLogMagic, kGDCTLogMagicSize));
    if (s.ok()) {
      s = gdct_log_writer_->Sync();
    }
  }
  return s;
}

Status HotspotManager::FlushGDCTLogBuffer() {
  if (pending_gdct_records_.load(std::memory_order_relaxed) == 0) {
    return Status::OK();
  }

  std::vector<GDCTDeleteRecord> local_buffer;
  {
    std::lock_guard<std::mutex> lock(gdct_append_mutex_);
    if (gdct_append_buffer_.empty()) {
      pending_gdct_records_.store(0, std::memory_order_relaxed);
      return Status::OK();
    }
    local_buffer = std::move(gdct_append_buffer_);
    gdct_append_buffer_.clear();
    pending_gdct_records_.store(0, std::memory_order_relaxed);
  }

  last_gdct_flush_time_us_.store(options_->env->NowMicros(),
                                 std::memory_order_relaxed);

  std::lock_guard<std::mutex> lock(gdct_log_mutex_);
  if (!gdct_log_writer_) {
    Status init = InitGDCTLogLocked();
    if (!init.ok()) {
      return init;
    }
  }

  Status s;
  for (const auto& record : local_buffer) {
    s = AppendWalRecordLocked(record);
    if (!s.ok()) {
      return s;
    }
  }
  return gdct_log_writer_->Sync();
}

Status HotspotManager::AppendWalRecordLocked(const GDCTDeleteRecord& record) {
  std::string encoded_record;
  EncodeWalRecord(record, &encoded_record);
  return gdct_log_writer_->Append(Slice(encoded_record));
}

Status HotspotManager::PersistDelete(const GDCTDeleteRecord& record,
                                     bool sync) {
  if (sync) {
    Status flush = FlushGDCTLogBuffer();
    if (!flush.ok()) {
      return flush;
    }

    std::lock_guard<std::mutex> lock(gdct_log_mutex_);
    if (!gdct_log_writer_) {
      Status init = InitGDCTLogLocked();
      if (!init.ok()) {
        return init;
      }
    }

    Status s = AppendWalRecordLocked(record);
    if (s.ok()) {
      s = gdct_log_writer_->Sync();
    }
    return s;
  }

  {
    std::lock_guard<std::mutex> lock(gdct_append_mutex_);
    gdct_append_buffer_.push_back(record);
  }
  pending_gdct_records_.fetch_add(1, std::memory_order_relaxed);
  return Status::OK();
}

void HotspotManager::RecoverGDCT() {
  std::string log_dir = data_dir_ + "/hot_shared_";
  std::string log_path = log_dir + "/gdct.log";

  std::unique_ptr<SequentialFile> file;
  EnvOptions env_options;
  Status s = options_->env->NewSequentialFile(log_path, &file, env_options);
  if (!s.ok()) {
    if (!s.IsNotFound()) {
      ROCKS_LOG_WARN(options_->info_log,
                     "[HotspotManager] Failed to open GDCT log for recovery: %s",
                     s.ToString().c_str());
    }
    return;
  }

  char magic_buffer[kGDCTLogMagicSize] = {};
  Slice magic_result;
  s = file->Read(kGDCTLogMagicSize, &magic_result, magic_buffer);
  if (!s.ok() || magic_result.size() < kGDCTLogMagicSize) {
    return;
  }
  if (std::memcmp(magic_result.data(), kGDCTLogMagic, kGDCTLogMagicSize) != 0) {
    ROCKS_LOG_WARN(options_->info_log,
                   "[HotspotManager] Legacy GDCT log format detected. Recovery skipped.");
    return;
  }

  size_t recovered_count = 0;
  uint64_t max_publish_epoch = 0;
  char buffer[kGDCTWalRecordSize] = {};
  Slice result;
  while (true) {
    s = file->Read(kGDCTWalRecordSize, &result, buffer);
    if (!s.ok() || result.size() < kGDCTWalRecordSize) {
      break;
    }

    GDCTDeleteRecord record;
    if (!DecodeWalRecord(result, &record)) {
      ROCKS_LOG_WARN(options_->info_log,
                     "[HotspotManager] Corrupted GDCT WAL record detected. Stop replay.");
      break;
    }
    if (record.record_type == GDCTRecordType::kMergeCommit) {
      delete_table_.PublishRecord(record.key, record.cutoff_seq,
                                  record.publish_epoch);
      if (record.publish_epoch > max_publish_epoch) {
        max_publish_epoch = record.publish_epoch;
      }
    } else if (record.record_type == GDCTRecordType::kPrunedCutoff) {
      delete_table_.SetPrunedCutoff(record.key, record.cutoff_seq);
    }
    ++recovered_count;
  }

  current_epoch_.store(max_publish_epoch, std::memory_order_release);
  published_epoch_.store(max_publish_epoch, std::memory_order_release);

  ROCKS_LOG_INFO(
      options_->info_log,
      "[HotspotManager] Recovered %zu CUID delete records from GDCT log.",
      recovered_count);
}

void HotspotManager::CompactAndFlushGDCTLogIfNeeded() {
  delete_table_.PruneObsoleteRecords(GetOldestActiveReadEpoch());

  uint64_t now = options_->env->NowMicros();
  if (pending_gdct_records_.load(std::memory_order_relaxed) <
          delta_options_.gdct_flush_threshold_records &&
      now - last_gdct_flush_time_us_.load(std::memory_order_relaxed) <
          delta_options_.gdct_flush_interval_us) {
    return;
  }

  Status flush = FlushGDCTLogBuffer();
  if (!flush.ok()) {
    return;
  }

  if (now - last_gdct_compact_time_us_.load(std::memory_order_relaxed) <
      delta_options_.gdct_compact_interval_us) {
    return;
  }
  last_gdct_compact_time_us_.store(now, std::memory_order_relaxed);

  std::string log_dir = data_dir_ + "/hot_shared_";
  std::string log_path = log_dir + "/gdct.log";

  uint64_t file_size = 0;
  Status s = options_->env->GetFileSize(log_path, &file_size);
  if (!s.ok() ||
      file_size < delta_options_.gdct_log_compact_size) {
    return;
  }

  ROCKS_LOG_INFO(options_->info_log,
                 "[HotspotManager] GDCT log size %" PRIu64
                 " bytes, starting compaction.",
                 file_size);

  std::string new_log_path = log_dir + "/gdct.log.new";
  std::unique_ptr<WritableFile> new_writer;
  EnvOptions env_options;
  s = options_->env->NewWritableFile(new_log_path, &new_writer, env_options);
  if (!s.ok()) {
    return;
  }

  s = new_writer->Append(Slice(kGDCTLogMagic, kGDCTLogMagicSize));
  if (!s.ok()) {
    return;
  }

  auto deleted_records = delete_table_.GetAllDeleteRecords();
  for (const auto& record : deleted_records) {
    std::string encoded_record;
    EncodeWalRecord(record, &encoded_record);
    s = new_writer->Append(Slice(encoded_record));
    if (!s.ok()) {
      break;
    }
  }

  if (s.ok()) {
    s = new_writer->Sync();
  }
  if (s.ok()) {
    s = new_writer->Close();
  } else {
    new_writer->Close();
  }
  if (!s.ok()) {
    return;
  }

  std::lock_guard<std::mutex> lock(gdct_log_mutex_);
  if (gdct_log_writer_) {
    gdct_log_writer_->Close();
    gdct_log_writer_.reset();
  }

  s = options_->env->RenameFile(new_log_path, log_path);
  if (!s.ok()) {
    ROCKS_LOG_WARN(options_->info_log,
                   "[HotspotManager] Failed to swap compacted GDCT log: %s",
                   s.ToString().c_str());
    return;
  }

  Status reopen =
      options_->env->ReopenWritableFile(log_path, &gdct_log_writer_, env_options);
  if (reopen.IsNotSupported()) {
    reopen = options_->env->NewWritableFile(log_path, &gdct_log_writer_, env_options);
  }
  if (!reopen.ok()) {
    ROCKS_LOG_WARN(options_->info_log,
                   "[HotspotManager] Failed to reopen compacted GDCT log: %s",
                   reopen.ToString().c_str());
    return;
  }

  ROCKS_LOG_INFO(options_->info_log,
                 "[HotspotManager] Successfully compacted GDCT log.");
}

}  // namespace ROCKSDB_NAMESPACE
