#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "delta/global_delete_count_table.h"
#include "options/cf_options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

class WritableFile;

struct HotspotManagerReadEpochSlot {
  std::atomic<uint64_t> pinned_epoch{0};
};

class HotspotManager {
 public:
  HotspotManager(const ImmutableOptions& options, const std::string& data_dir,
                 const std::string& delta_cf_name,
                 const InternalKeyComparator* internal_comparator);

  ~HotspotManager() = default;

  bool IsDeltaColumnFamily(const std::string& cf_name) const {
    return cf_name == delta_cf_name_;
  }

  const std::string& GetDeltaColumnFamilyName() const {
    return delta_cf_name_;
  }

  uint64_t CurrentEpoch() const {
    return published_epoch_.load(std::memory_order_acquire);
  }

  uint64_t AllocateEpoch();

  uint64_t FreezeReadEpoch() const;

  uint64_t AcquireReadEpoch();
  void AcquireReadEpoch(uint64_t epoch);
  void ReleaseReadEpoch(uint64_t epoch);

  uint64_t GetPublishBarrier() const {
    return publish_barrier_.load(std::memory_order_acquire);
  }

  void PublishEpoch(uint64_t epoch);

  void UpdateOldestActiveReadEpoch(uint64_t epoch) {
    oldest_active_read_epoch_.store(epoch, std::memory_order_release);
  }

  uint64_t GetOldestActiveReadEpoch() const;

  bool ShouldHideForRead(const Slice& key, SequenceNumber put_seq,
                         uint64_t read_epoch) const;
  bool ShouldDropDuringCompaction(const Slice& key, SequenceNumber put_seq,
                                  uint64_t oldest_active_read_epoch) const;

  bool TrackPhysicalUnit(const Slice& key, uint64_t phys_id);
  bool TrackPhysicalUnit(const GDCTKey& key, uint64_t phys_id) {
    return delete_table_.TrackPhysicalUnit(key, phys_id);
  }

  void UntrackFiles(const GDCTKey& key, const std::vector<uint64_t>& file_ids) {
    delete_table_.UntrackFiles(key, file_ids);
  }

  Status InterceptDelete(const Slice& key, SequenceNumber seq, bool sync);
  Status PersistDelete(const GDCTDeleteRecord& record, bool sync);

  void RecoverGDCT();
  void CompactAndFlushGDCTLogIfNeeded();

  bool ExtractGDCTKey(const Slice& key, GDCTKey* gdct_key) const;
  uint64_t ExtractCUID(const Slice& key) const;

  void UpdateCompactionRefCount(
      const GDCTKey& key, int32_t input_count, int32_t output_count,
      const std::vector<uint64_t>& input_files_verified,
      uint64_t output_file_verified) {
    delete_table_.ApplyCompactionChange(key, input_count, output_count,
                                        input_files_verified,
                                        output_file_verified);
  }

  void UpdateCompactionRefCount(
      const GDCTKey& key, const std::vector<uint64_t>& input_files,
      const std::vector<GDCTFileRef>& output_files) {
    delete_table_.ApplyCompactionChange(key, input_files, output_files);
  }

  void UpdateFlushRefCount(const GDCTKey& key, uint64_t output_file) {
    GDCTFileRef file_ref;
    file_ref.file_id = output_file;
    delete_table_.ApplyFlushChange(key, file_ref);
  }

  void UpdateFlushRefCount(const GDCTKey& key, const GDCTFileRef& output_file) {
    delete_table_.ApplyFlushChange(key, output_file);
  }

  GlobalDeleteCountTable& GetDeleteTable() { return delete_table_; }
  const GlobalDeleteCountTable& GetDeleteTable() const { return delete_table_; }

  bool HasPendingGDCTFlush() const {
    return pending_gdct_records_.load(std::memory_order_relaxed) > 0;
  }

 private:
  HotspotManagerReadEpochSlot* RegisterReadEpochSlot();
  uint64_t RefreshOldestActiveReadEpoch() const;

  Status InitGDCTLog();
  Status InitGDCTLogLocked();
  Status FlushGDCTLogBuffer();
  Status AppendWalRecordLocked(const GDCTDeleteRecord& record);

  const ImmutableOptions* options_;
  std::string data_dir_;
  std::string delta_cf_name_;
  const InternalKeyComparator* internal_comparator_;
  DeltaOptions delta_options_;
  GlobalDeleteCountTable delete_table_;

  std::unique_ptr<WritableFile> gdct_log_writer_;
  mutable std::mutex gdct_log_mutex_;

  std::vector<GDCTDeleteRecord> gdct_append_buffer_;
  std::mutex gdct_append_mutex_;
  std::atomic<uint64_t> pending_gdct_records_{0};
  std::atomic<uint64_t> last_gdct_flush_time_us_{0};
  std::atomic<uint64_t> last_gdct_compact_time_us_{0};
  std::atomic<uint64_t> current_epoch_{0};
  std::atomic<uint64_t> published_epoch_{0};
  std::atomic<uint64_t> publish_barrier_{0};
  mutable std::atomic<uint64_t> oldest_active_read_epoch_{0};
  mutable std::mutex read_epoch_slots_mutex_;
  std::vector<std::unique_ptr<HotspotManagerReadEpochSlot>> read_epoch_slots_;
};

}  // namespace ROCKSDB_NAMESPACE