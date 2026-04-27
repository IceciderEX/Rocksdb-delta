#include "delta/global_delete_count_table.h"

#include <algorithm>
#include <mutex>

namespace {

using ROCKSDB_NAMESPACE::GDCTDeleteRecord;
using ROCKSDB_NAMESPACE::GDCTEntry;
using ROCKSDB_NAMESPACE::GDCTFileRef;
using ROCKSDB_NAMESPACE::GDCTKey;
using ROCKSDB_NAMESPACE::GDCTRecord;
using ROCKSDB_NAMESPACE::GDCTRecordType;
using ROCKSDB_NAMESPACE::SequenceNumber;

template <typename VectorType>
auto FindFileRef(VectorType& refs, uint64_t file_id) {
  return std::lower_bound(
      refs.begin(), refs.end(), file_id,
      [](const GDCTFileRef& ref, uint64_t id) { return ref.file_id < id; });
}

template <typename VectorType>
auto FindActiveRecord(VectorType& records, uint64_t publish_epoch,
                      SequenceNumber cutoff_seq) {
  return std::lower_bound(
      records.begin(), records.end(), std::make_pair(publish_epoch, cutoff_seq),
      [](const GDCTRecord& record,
         const std::pair<uint64_t, SequenceNumber>& target) {
        if (record.publish_epoch != target.first) {
          return record.publish_epoch < target.first;
        }
        return record.cutoff_seq < target.second;
      });
}

void RecomputeRefCount(GDCTEntry* entry) {
  entry->ref_count = static_cast<int32_t>(entry->file_refs.size());
}

void UpsertFileRef(GDCTEntry* entry, const GDCTFileRef& file_ref) {
  auto it = FindFileRef(entry->file_refs, file_ref.file_id);
  if (it == entry->file_refs.end() || it->file_id != file_ref.file_id) {
    entry->file_refs.insert(it, file_ref);
  }
  RecomputeRefCount(entry);
}

void RemoveFileRef(GDCTEntry* entry, uint64_t file_id) {
  auto it = FindFileRef(entry->file_refs, file_id);
  if (it != entry->file_refs.end() && it->file_id == file_id) {
    entry->file_refs.erase(it);
    RecomputeRefCount(entry);
  }
}

}  // namespace

namespace ROCKSDB_NAMESPACE {

GlobalDeleteCountTable::GlobalDeleteCountTable(size_t num_shards)
    : shards_(num_shards == 0 ? 1 : num_shards) {}

bool GlobalDeleteCountTable::TrackPhysicalUnit(const GDCTKey& key,
                                               uint64_t phys_id) {
  GDCTFileRef file_ref;
  file_ref.file_id = phys_id;
  return TrackPhysicalUnit(key, file_ref);
}

bool GlobalDeleteCountTable::TrackPhysicalUnit(const GDCTKey& key,
                                               const GDCTFileRef& file_ref) {
  auto& shard = GetShard(key);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto& entry = shard.table[key];

  auto it = FindFileRef(entry.file_refs, file_ref.file_id);
  if (it == entry.file_refs.end() || it->file_id != file_ref.file_id) {
    entry.file_refs.insert(it, file_ref);
    RecomputeRefCount(&entry);
    return true;
  }
  return false;
}

void GlobalDeleteCountTable::ResetTracking(const GDCTKey& key) {
  auto& shard = GetShard(key);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(key);
  if (it != shard.table.end()) {
    it->second.ref_count = 0;
    it->second.file_refs.clear();
  }
}

void GlobalDeleteCountTable::UntrackFiles(const GDCTKey& key,
                                          const std::vector<uint64_t>& file_ids) {
  auto& shard = GetShard(key);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(key);
  if (it == shard.table.end()) return;

  auto& entry = it->second;
  for (uint64_t fid : file_ids) {
    RemoveFileRef(&entry, fid);
  }
}

void GlobalDeleteCountTable::ApplyCompactionChange(
    const GDCTKey& key, int32_t input_count, int32_t output_count,
    const std::vector<uint64_t>& input_files, uint64_t output_file) {
  std::vector<GDCTFileRef> output_files;
  if (output_file != 0) {
    GDCTFileRef file_ref;
    file_ref.file_id = output_file;
    output_files.push_back(file_ref);
  }

  ApplyCompactionChange(key, input_files, output_files);
  (void)input_count;
  (void)output_count;
}

void GlobalDeleteCountTable::ApplyCompactionChange(
    const GDCTKey& key, const std::vector<uint64_t>& input_files,
    const std::vector<GDCTFileRef>& output_files) {
  auto& shard = GetShard(key);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(key);

  if (it == shard.table.end()) {
    if (!output_files.empty()) {
      auto& new_entry = shard.table[key];
      for (const auto& output : output_files) {
        UpsertFileRef(&new_entry, output);
      }
    }
    return;
  }

  auto& entry = it->second;
  for (uint64_t fid : input_files) {
    RemoveFileRef(&entry, fid);
  }

  for (const auto& output : output_files) {
    UpsertFileRef(&entry, output);
  }
}

void GlobalDeleteCountTable::ApplyFlushChange(const GDCTKey& key,
                                              uint64_t output_file) {
  GDCTFileRef file_ref;
  file_ref.file_id = output_file;
  ApplyFlushChange(key, file_ref);
}

void GlobalDeleteCountTable::ApplyFlushChange(const GDCTKey& key,
                                              const GDCTFileRef& output_file) {
  auto& shard = GetShard(key);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(key);
  if (it == shard.table.end()) return;

  auto& entry = it->second;
  UpsertFileRef(&entry, output_file);
}

bool GlobalDeleteCountTable::PublishRecord(const GDCTKey& key,
                                           SequenceNumber cutoff_seq,
                                           uint64_t publish_epoch,
                                           bool* newly_published) {
  auto& shard = GetShard(key);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto& entry = shard.table[key];

  auto it = FindActiveRecord(entry.active_records, publish_epoch, cutoff_seq);
  bool inserted = false;
  if (it == entry.active_records.end() || it->publish_epoch != publish_epoch ||
      it->cutoff_seq != cutoff_seq) {
    entry.active_records.insert(it, GDCTRecord{cutoff_seq, publish_epoch});
    inserted = true;
  }
  if (publish_epoch > entry.max_publish_epoch) {
    entry.max_publish_epoch = publish_epoch;
  }
  if (newly_published) {
    *newly_published = inserted;
  }
  return true;
}

void GlobalDeleteCountTable::SetPrunedCutoff(const GDCTKey& key,
                                             SequenceNumber cutoff_seq) {
  auto& shard = GetShard(key);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto& entry = shard.table[key];
  if (cutoff_seq > entry.max_pruned_cutoff_seq) {
    entry.max_pruned_cutoff_seq = cutoff_seq;
  }
  entry.active_records.erase(
      std::remove_if(entry.active_records.begin(), entry.active_records.end(),
                     [&](const GDCTRecord& record) {
                       return record.cutoff_seq <= entry.max_pruned_cutoff_seq;
                     }),
      entry.active_records.end());
  if (!entry.active_records.empty()) {
    entry.max_publish_epoch = entry.active_records.back().publish_epoch;
  } else {
    entry.max_publish_epoch = 0;
  }
}

void GlobalDeleteCountTable::PruneObsoleteRecords(
    uint64_t oldest_active_read_epoch) {
  for (auto& shard : shards_) {
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    for (auto it = shard.table.begin(); it != shard.table.end();) {
      auto& entry = it->second;
      SequenceNumber max_pruned_cutoff = entry.max_pruned_cutoff_seq;
      auto erase_end = entry.active_records.begin();
      while (erase_end != entry.active_records.end() &&
             erase_end->publish_epoch <= oldest_active_read_epoch) {
        if (erase_end->cutoff_seq > max_pruned_cutoff) {
          max_pruned_cutoff = erase_end->cutoff_seq;
        }
        ++erase_end;
      }
      if (erase_end != entry.active_records.begin()) {
        entry.active_records.erase(entry.active_records.begin(), erase_end);
        entry.max_pruned_cutoff_seq = max_pruned_cutoff;
      }
      if (entry.active_records.empty()) {
        entry.max_publish_epoch = 0;
      } else {
        entry.max_publish_epoch = entry.active_records.back().publish_epoch;
      }
      if (entry.Empty()) {
        it = shard.table.erase(it);
      } else {
        ++it;
      }
    }
  }
}

bool GlobalDeleteCountTable::ShouldHideForRead(const GDCTKey& key,
                                               SequenceNumber put_seq,
                                               uint64_t read_epoch) const {
  if (put_seq == 0) {
    return false;
  }
  const auto& shard = GetShard(key);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(key);
  if (it == shard.table.end()) {
    return false;
  }

  const auto& entry = it->second;
  if (put_seq <= entry.max_pruned_cutoff_seq) {
    return true;
  }

  for (const auto& record : entry.active_records) {
    if (record.publish_epoch > read_epoch) {
      break;
    }
    if (put_seq <= record.cutoff_seq) {
      return true;
    }
  }
  return false;
}

bool GlobalDeleteCountTable::ShouldDropDuringCompaction(
    const GDCTKey& key, SequenceNumber put_seq,
    uint64_t oldest_active_read_epoch) const {
  if (put_seq == 0) {
    return false;
  }
  const auto& shard = GetShard(key);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(key);
  if (it == shard.table.end()) {
    return false;
  }

  const auto& entry = it->second;
  if (put_seq <= entry.max_pruned_cutoff_seq) {
    return true;
  }

  for (const auto& record : entry.active_records) {
    if (record.publish_epoch > oldest_active_read_epoch) {
      break;
    }
    if (put_seq <= record.cutoff_seq) {
      return true;
    }
  }
  return false;
}

SequenceNumber GlobalDeleteCountTable::GetMaxPrunedCutoffSeq(
    const GDCTKey& key) const {
  const auto& shard = GetShard(key);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(key);
  if (it != shard.table.end()) {
    return it->second.max_pruned_cutoff_seq;
  }
  return 0;
}

uint64_t GlobalDeleteCountTable::GetLatestPublishEpoch(const GDCTKey& key) const {
  const auto& shard = GetShard(key);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(key);
  if (it != shard.table.end()) {
    return it->second.max_publish_epoch;
  }
  return 0;
}

std::vector<GDCTDeleteRecord> GlobalDeleteCountTable::GetAllDeleteRecords() const {
  std::vector<GDCTDeleteRecord> result;
  for (const auto& shard : shards_) {
    std::shared_lock<std::shared_mutex> lock(shard.mutex);
    for (const auto& kv : shard.table) {
      const auto& key = kv.first;
      const auto& entry = kv.second;
      if (entry.max_pruned_cutoff_seq > 0) {
        result.push_back(GDCTDeleteRecord{GDCTRecordType::kPrunedCutoff, key,
                                          entry.max_pruned_cutoff_seq, 0});
      }
      for (const auto& record : entry.active_records) {
        result.push_back(GDCTDeleteRecord{GDCTRecordType::kMergeCommit, key,
                                          record.cutoff_seq,
                                          record.publish_epoch});
      }
    }
  }
  std::sort(result.begin(), result.end(),
            [](const GDCTDeleteRecord& lhs, const GDCTDeleteRecord& rhs) {
    if (lhs.key < rhs.key) {
      return true;
    }
    if (rhs.key < lhs.key) {
      return false;
    }
    if (lhs.record_type != rhs.record_type) {
      return static_cast<uint8_t>(lhs.record_type) <
             static_cast<uint8_t>(rhs.record_type);
    }
    if (lhs.publish_epoch != rhs.publish_epoch) {
      return lhs.publish_epoch < rhs.publish_epoch;
    }
    return lhs.cutoff_seq < rhs.cutoff_seq;
  });
  return result;
}

int GlobalDeleteCountTable::GetRefCount(const GDCTKey& key) const {
  const auto& shard = GetShard(key);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(key);
  if (it != shard.table.end()) {
    return it->second.GetRefCount();
  }
  return 0;
}

bool GlobalDeleteCountTable::IsTracked(const GDCTKey& key) const {
  const auto& shard = GetShard(key);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  return shard.table.find(key) != shard.table.end();
}

}  // namespace ROCKSDB_NAMESPACE