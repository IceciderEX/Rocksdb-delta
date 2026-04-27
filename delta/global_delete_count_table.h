#pragma once

#include <cstddef>
#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "rocksdb/types.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct GDCTKey {
  uint64_t dbid = 0;
  uint64_t tableid = 0;
  uint64_t cuid = 0;

  bool operator==(const GDCTKey& other) const {
    return dbid == other.dbid && tableid == other.tableid &&
           cuid == other.cuid;
  }

  bool operator<(const GDCTKey& other) const {
    if (dbid != other.dbid) {
      return dbid < other.dbid;
    }
    if (tableid != other.tableid) {
      return tableid < other.tableid;
    }
    return cuid < other.cuid;
  }
};

struct GDCTKeyHash {
  size_t operator()(const GDCTKey& key) const {
    size_t hash = std::hash<uint64_t>{}(key.dbid);
    hash ^= std::hash<uint64_t>{}(key.tableid) + 0x9e3779b97f4a7c15ULL +
            (hash << 6) + (hash >> 2);
    hash ^= std::hash<uint64_t>{}(key.cuid) + 0x9e3779b97f4a7c15ULL +
            (hash << 6) + (hash >> 2);
    return hash;
  }
};

enum class GDCTRecordType : uint8_t {
  kMergeCommit = 1,
  kPrunedCutoff = 2,
};

struct GDCTRecord {
  SequenceNumber cutoff_seq = 0;
  uint64_t publish_epoch = 0;
};

struct GDCTDeleteRecord {
  GDCTRecordType record_type = GDCTRecordType::kMergeCommit;
  GDCTKey key;
  SequenceNumber cutoff_seq = 0;
  uint64_t publish_epoch = 0;
};

struct GDCTFileRef {
  uint64_t file_id = 0;
};

struct GDCTEntry {
  SequenceNumber max_pruned_cutoff_seq = 0;
  std::vector<GDCTRecord> active_records;
  std::vector<GDCTFileRef> file_refs;
  int32_t ref_count = 0;
  uint64_t max_publish_epoch = 0;

  int GetRefCount() const { return ref_count; }
  bool Empty() const { return active_records.empty() && file_refs.empty(); }
};

class GlobalDeleteCountTable {
 public:
  explicit GlobalDeleteCountTable(size_t num_shards = 128);

  bool TrackPhysicalUnit(const GDCTKey& key, uint64_t phys_id);
  bool TrackPhysicalUnit(const GDCTKey& key, const GDCTFileRef& file_ref);

  void UntrackFiles(const GDCTKey& key, const std::vector<uint64_t>& file_ids);
  void ResetTracking(const GDCTKey& key);
  bool IsTracked(const GDCTKey& key) const;

  bool PublishRecord(const GDCTKey& key, SequenceNumber cutoff_seq,
                     uint64_t publish_epoch,
                     bool* newly_published = nullptr);
  void SetPrunedCutoff(const GDCTKey& key, SequenceNumber cutoff_seq);
  void PruneObsoleteRecords(uint64_t oldest_active_read_epoch);

  bool ShouldHideForRead(const GDCTKey& key, SequenceNumber put_seq,
                         uint64_t read_epoch) const;
  bool ShouldDropDuringCompaction(
      const GDCTKey& key, SequenceNumber put_seq,
      uint64_t oldest_active_read_epoch) const;

  SequenceNumber GetMaxPrunedCutoffSeq(const GDCTKey& key) const;
  uint64_t GetLatestPublishEpoch(const GDCTKey& key) const;

  std::vector<GDCTDeleteRecord> GetAllDeleteRecords() const;

  int GetRefCount(const GDCTKey& key) const;

  void ApplyCompactionChange(const GDCTKey& key, int32_t input_count,
                             int32_t output_count,
                             const std::vector<uint64_t>& input_files,
                             uint64_t output_file);
  void ApplyCompactionChange(const GDCTKey& key,
                             const std::vector<uint64_t>& input_files,
                             const std::vector<GDCTFileRef>& output_files);

  void ApplyFlushChange(const GDCTKey& key, uint64_t output_file);
  void ApplyFlushChange(const GDCTKey& key, const GDCTFileRef& output_file);

  size_t GetShardCount() const { return shards_.size(); }

 private:
  struct Shard {
    mutable std::shared_mutex mutex;
    std::unordered_map<GDCTKey, GDCTEntry, GDCTKeyHash> table;
  };

  size_t HashKey(const GDCTKey& key) const {
    return GDCTKeyHash{}(key) % shards_.size();
  }

  Shard& GetShard(const GDCTKey& key) {
    return shards_[HashKey(key)];
  }

  const Shard& GetShard(const GDCTKey& key) const {
    return shards_[HashKey(key)];
  }

  std::vector<Shard> shards_;
};

}  // namespace ROCKSDB_NAMESPACE