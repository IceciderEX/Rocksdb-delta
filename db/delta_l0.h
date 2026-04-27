#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/version_edit.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

// DELTA_PARTITION_OBSERVABILITY_OR_TEST_ONLY: marker for non-functional
// observability/test scaffolding that can be removed later if desired.

// 判断给定 Column Family 名称是否为 Delta 专用列族。
// 返回 true 表示该列族需要启用 Delta L0 分区相关逻辑。
inline bool IsDeltaColumnFamilyName(const std::string& cf_name) {
  return cf_name == "Delta";
}

// Delta 固定宽度二进制 user key 各字段的偏移和长度定义。
// 布局为 dbid[4] relid[4] cuid[8] offset[4] udseq[8]，总长度为 28 字节。
constexpr size_t kDeltaBinaryKeyDbIdOffset = 0;
constexpr size_t kDeltaBinaryKeyDbIdSize = 4;
constexpr size_t kDeltaBinaryKeyRelIdOffset =
    kDeltaBinaryKeyDbIdOffset + kDeltaBinaryKeyDbIdSize;
constexpr size_t kDeltaBinaryKeyRelIdSize = 4;
constexpr size_t kDeltaBinaryKeyCuidOffset =
    kDeltaBinaryKeyRelIdOffset + kDeltaBinaryKeyRelIdSize;
constexpr size_t kDeltaBinaryKeyCuidSize = 8;
constexpr size_t kDeltaBinaryKeyOffsetOffset =
    kDeltaBinaryKeyCuidOffset + kDeltaBinaryKeyCuidSize;
constexpr size_t kDeltaBinaryKeyOffsetSize = 4;
constexpr size_t kDeltaBinaryKeyUdseqOffset =
    kDeltaBinaryKeyOffsetOffset + kDeltaBinaryKeyOffsetSize;
constexpr size_t kDeltaBinaryKeyUdseqSize = 8;
constexpr size_t kDeltaBinaryKeySize =
    kDeltaBinaryKeyUdseqOffset + kDeltaBinaryKeyUdseqSize;

// 按大端序从固定长度二进制 key 中解码 32 位无符号整数。
// 用于解析 Delta binary key 中的 dbid、relid、offset 等字段。
inline uint32_t DecodeDeltaBigEndian32(const char* ptr) {
  uint32_t value = 0;
  for (size_t i = 0; i < sizeof(uint32_t); ++i) {
    value = (value << 8) | static_cast<unsigned char>(ptr[i]);
  }
  return value;
}

// 按大端序从固定长度二进制 key 中解码 64 位无符号整数。
// 用于解析 Delta binary key 中的 cuid、udseq 等字段。
inline uint64_t DecodeDeltaBigEndian64(const char* ptr) {
  uint64_t value = 0;
  for (size_t i = 0; i < sizeof(uint64_t); ++i) {
    value = (value << 8) | static_cast<unsigned char>(ptr[i]);
  }
  return value;
}

// 尝试从真实 Delta 二进制 user key 中解析前缀字段。
// 当前只抽取 dbid、relid、cuid，这些字段会被分区和 GDCT 共同使用。
inline bool TryParseDeltaBinaryKeyPrefix(const Slice& user_key, uint32_t* dbid,
                                         uint32_t* relid, uint64_t* cuid) {
  if (user_key.size() != kDeltaBinaryKeySize) {
    return false;
  }

  if (dbid != nullptr) {
    *dbid = DecodeDeltaBigEndian32(user_key.data() + kDeltaBinaryKeyDbIdOffset);
  }
  if (relid != nullptr) {
    *relid =
        DecodeDeltaBigEndian32(user_key.data() + kDeltaBinaryKeyRelIdOffset);
  }
  if (cuid != nullptr) {
    *cuid = DecodeDeltaBigEndian64(user_key.data() + kDeltaBinaryKeyCuidOffset);
  }
  return true;
}

// 尝试从历史测试用的文本格式 user key 中解析 table id。
// 该函数只作为兼容旧测试数据的 fallback；真实 Delta key 使用固定宽度二进制格式。
inline bool TryParseLegacyDeltaTableIdFromUserKey(const Slice& user_key,
                                                  uint64_t* tableid) {
  // Legacy fallback for historical synthetic test data such as
  // "1_0001_1_0001_1". Real Delta keys are fixed-width binary.
  size_t pos = 0;
  while (pos < user_key.size() && user_key[pos] != '_') {
    ++pos;
  }
  if (pos == user_key.size()) {
    return false;
  }

  const size_t tableid_start = pos + 1;
  uint64_t parsed = 0;
  pos = tableid_start;
  while (pos < user_key.size() && user_key[pos] != '_') {
    const char c = user_key[pos];
    if (c < '0' || c > '9') {
      return false;
    }
    parsed = parsed * 10 + static_cast<uint64_t>(c - '0');
    ++pos;
  }
  if (pos == tableid_start) {
    return false;
  }

  *tableid = parsed;
  return true;
}

// 从用户 key 中解析 Delta L0 分区使用的 table id。
// Delta 实际 user key 格式为固定宽度二进制：
//   dbid[4] relid[4] cuid[8] offset[4] udseq[8]
// 当前分区逻辑使用 relid 作为 table id。
// 如果不是二进制 key，则回退到历史测试数据使用的文本格式解析。
inline bool TryParseDeltaTableIdFromUserKey(const Slice& user_key,
                                            uint64_t* tableid) {
  uint32_t relid = 0;
  if (TryParseDeltaBinaryKeyPrefix(user_key, nullptr, &relid, nullptr)) {
    *tableid = relid;
    return true;
  }
  return TryParseLegacyDeltaTableIdFromUserKey(user_key, tableid);
}

// 根据 SST 文件的 smallest/largest user key 推导 Delta L0 文件元信息。
// 如果边界 key 无效、无法解析 table id，或解析出的范围非法，则返回 invalid meta。
inline DeltaL0FileMeta BuildDeltaL0FileMetaFromBounds(
    const FileMetaData& file_meta) {
  uint64_t tableid_min = 0;
  uint64_t tableid_max = 0;
  if (!file_meta.smallest.Valid() || !file_meta.largest.Valid() ||
      !TryParseDeltaTableIdFromUserKey(file_meta.smallest.user_key(),
                                       &tableid_min) ||
      !TryParseDeltaTableIdFromUserKey(file_meta.largest.user_key(),
                                       &tableid_max) ||
      tableid_min > tableid_max) {
    return {};
  }

  DeltaL0FileMeta meta;
  meta.valid = true;
  meta.tableid_min = tableid_min;
  meta.tableid_max = tableid_max;
  return meta;
}

// 按条件填充 FileMetaData 中的 Delta L0 元信息。
// 仅对 Delta Column Family 的 L0 文件生成 table id 范围；其他情况清空该元信息。
inline void MaybePopulateDeltaL0FileMeta(const std::string& cf_name, int level,
                                         FileMetaData* file_meta) {
  if (level != 0 || !IsDeltaColumnFamilyName(cf_name)) {
    file_meta->delta_l0_meta = {};
    return;
  }
  file_meta->delta_l0_meta = BuildDeltaL0FileMetaFromBounds(*file_meta);
}

// 判断某个 Delta L0 文件是否可能包含指定 table id。
// 对缺少有效 Delta L0 元信息的旧文件采取保守策略，返回 true 以避免误剪枝。
inline bool DeltaL0FileMayContainTableId(const FileMetaData& file_meta,
                                         uint64_t tableid) {
  return !file_meta.delta_l0_meta.valid ||
         (file_meta.delta_l0_meta.tableid_min <= tableid &&
          tableid <= file_meta.delta_l0_meta.tableid_max);
}

// 判断某个 Delta L0 文件是否可能与指定 table id 范围重叠。
// 对缺少有效 Delta L0 元信息的旧文件采取保守策略，认为可能重叠。
inline bool DeltaL0FileMayOverlapTableIdRange(const FileMetaData& file_meta,
                                              uint64_t tableid_min,
                                              uint64_t tableid_max) {
  return !file_meta.delta_l0_meta.valid ||
         !(file_meta.delta_l0_meta.tableid_max < tableid_min ||
           file_meta.delta_l0_meta.tableid_min > tableid_max);
}

// 表示 Delta L0 中的一个 table id 分区。
// 每个分区覆盖一个连续 table id 区间，并记录已经观察到的 table id。
struct DeltaL0Partition {
  uint64_t partition_id = 0;
  uint64_t generation = 0;
  uint64_t tableid_begin = 0;
  uint64_t tableid_end = std::numeric_limits<uint64_t>::max();
  std::set<uint64_t> observed_tableids;

  bool Contains(uint64_t tableid) const {
    return tableid_begin <= tableid && tableid <= tableid_end;
  }
};

// 记录读路径中 L0 候选文件剪枝前后的累计计数。
// 使用原子变量便于多个读线程并发更新观测指标。
struct DeltaL0ReadPruneCounters {
  std::atomic<uint64_t> get_l0_candidate_files_before_prune{0};
  std::atomic<uint64_t> get_l0_candidate_files_after_prune{0};
  std::atomic<uint64_t> scan_l0_candidate_files_before_prune{0};
  std::atomic<uint64_t> scan_l0_candidate_files_after_prune{0};
};

// Delta L0 读剪枝计数器的普通数值快照。
// 用于对外展示或测试断言，避免直接暴露内部 atomic 变量。
struct DeltaL0ReadPruneCountersSnapshot {
  uint64_t get_l0_candidate_files_before_prune = 0;
  uint64_t get_l0_candidate_files_after_prune = 0;
  uint64_t scan_l0_candidate_files_before_prune = 0;
  uint64_t scan_l0_candidate_files_after_prune = 0;
};

// 维护 Delta L0 table id 分区目录。
// 该目录支持按 table id 查找分区、观察访问模式后自动拆分、编码/恢复快照，以及记录读剪枝指标。
class DeltaL0PartitionDirectory {
 public:
  static constexpr uint64_t kSnapshotFormatVersion = 1;
  static constexpr uint64_t kTargetTablesPerPartition = 32;
  static constexpr uint64_t kSplitFactor = 2;
  static constexpr uint64_t kMinTablesToSplit = 16;
  static constexpr size_t kMaxPartitionCount = 256;

  // 构造函数：初始化为覆盖整个 table id 空间的单一分区。
  DeltaL0PartitionDirectory() { ResetToSinglePartition(); }

  // 查找指定 table id 当前所属的分区。
  // 返回内部 partitions_ 中的只读引用，调用方不应修改其内容。
  const DeltaL0Partition& Lookup(uint64_t tableid) const {
    return partitions_[FindPartitionIndex(tableid)];
  }

  // 先记录一次对 table id 的观察，再返回其所属分区。
  // 记录观察可能触发分区拆分，因此返回值使用拷贝以避免引用失效。
  DeltaL0Partition LookupAndRegister(uint64_t tableid) {
    Observe(tableid);
    return Lookup(tableid);
  }

  // 将 table id 记录到当前所属分区的 observed_tableids 中。
  // 当分区内观察到的 table id 过多时，尝试触发分区拆分。
  void Observe(uint64_t tableid) {
    const size_t index = FindPartitionIndex(tableid);
    auto& partition = partitions_[index];
    partition.observed_tableids.insert(tableid);
    MaybeSplit(index);
  }

  // 返回当前分区数量。
  size_t PartitionCount() const { return partitions_.size(); }

  // 返回目录生命周期内已经成功执行的分区拆分次数。
  uint64_t SplitCount() const { return split_count_; }

  // 返回当前分区列表的拷贝快照。
  // 调用方可以安全读取该快照，而不会影响目录内部状态。
  std::vector<DeltaL0Partition> GetPartitionsSnapshot() const {
    return partitions_;
  }

  // 将当前分区目录编码为二进制快照。
  // 快照包含格式版本、下一个分区 ID、拆分次数、各分区边界以及已观察到的 table id。
  std::string EncodeSnapshot() const {
    std::string encoded;
    PutVarint64(&encoded, kSnapshotFormatVersion);
    PutVarint64(&encoded, next_partition_id_);
    PutVarint64(&encoded, split_count_);
    PutVarint64(&encoded, static_cast<uint64_t>(partitions_.size()));
    for (const DeltaL0Partition& partition : partitions_) {
      PutVarint64(&encoded, partition.partition_id);
      PutVarint64(&encoded, partition.generation);
      PutVarint64(&encoded, partition.tableid_begin);
      PutVarint64(&encoded, partition.tableid_end);
      PutVarint64(&encoded,
                  static_cast<uint64_t>(partition.observed_tableids.size()));
      for (uint64_t observed_tableid : partition.observed_tableids) {
        PutVarint64(&encoded, observed_tableid);
      }
    }
    return encoded;
  }

  // 返回当前编码快照的字节大小。
  // 注意该函数会实际执行一次 EncodeSnapshot()，因此成本与分区数量和观察集合大小相关。
  size_t GetEncodedSnapshotSize() const { return EncodeSnapshot().size(); }

  // 从 EncodeSnapshot() 生成的二进制快照恢复分区目录状态。
  // 恢复前会校验格式版本、分区数量、分区连续性、观察值合法性以及 next_partition_id。
  bool RestoreFromSnapshot(const Slice& encoded_snapshot) {
    Slice input(encoded_snapshot);
    uint64_t format_version = 0;
    uint64_t next_partition_id = 0;
    uint64_t split_count = 0;
    uint64_t partition_count = 0;
    if (!GetVarint64(&input, &format_version) ||
        format_version != kSnapshotFormatVersion ||
        !GetVarint64(&input, &next_partition_id) ||
        !GetVarint64(&input, &split_count) ||
        !GetVarint64(&input, &partition_count) ||
        partition_count == 0 || partition_count > kMaxPartitionCount) {
      return false;
    }

    std::vector<DeltaL0Partition> restored;
    restored.reserve(static_cast<size_t>(partition_count));
    for (uint64_t partition_index = 0; partition_index < partition_count;
         ++partition_index) {
      DeltaL0Partition partition;
      uint64_t observed_count = 0;
      if (!GetVarint64(&input, &partition.partition_id) ||
          !GetVarint64(&input, &partition.generation) ||
          !GetVarint64(&input, &partition.tableid_begin) ||
          !GetVarint64(&input, &partition.tableid_end) ||
          !GetVarint64(&input, &observed_count)) {
        return false;
      }
      for (uint64_t observed_index = 0; observed_index < observed_count;
           ++observed_index) {
        uint64_t observed_tableid = 0;
        if (!GetVarint64(&input, &observed_tableid) ||
            !partition.Contains(observed_tableid)) {
          return false;
        }
        partition.observed_tableids.insert(observed_tableid);
      }
      restored.push_back(std::move(partition));
    }
    if (!input.empty() ||
        !ValidatePartitions(restored, next_partition_id)) {
      return false;
    }

    partitions_ = std::move(restored);
    next_partition_id_ = next_partition_id;
    split_count_ = split_count;
    return true;
  }

  // 根据现有 L0 文件中的 Delta L0 元信息重建分区目录。
  // 该函数用于没有可用目录快照时的兜底恢复：按 partition_id/generation 聚合文件，选择不重叠候选分区并重建连续区间。
  void RestoreFromL0Files(const std::vector<FileMetaData*>& level0_files) {
    struct RecoveredPartition {
      uint64_t partition_id = 0;
      uint64_t generation = 0;
      uint64_t min_tableid = 0;
      uint64_t max_tableid = 0;
      std::set<uint64_t> observed_tableids;
    };

    std::map<std::pair<uint64_t, uint64_t>, RecoveredPartition> grouped;
    for (const FileMetaData* file_meta : level0_files) {
      if (file_meta == nullptr ||
          !file_meta->delta_l0_meta.valid ||
          file_meta->delta_l0_meta.partition_id == 0 ||
          file_meta->delta_l0_meta.tableid_min >
              file_meta->delta_l0_meta.tableid_max) {
        continue;
      }

      const auto key = std::make_pair(file_meta->delta_l0_meta.partition_id,
                                      file_meta->delta_l0_meta.partition_generation);
      auto iter = grouped.find(key);
      if (iter == grouped.end()) {
        RecoveredPartition recovered;
        recovered.partition_id = file_meta->delta_l0_meta.partition_id;
        recovered.generation = file_meta->delta_l0_meta.partition_generation;
        recovered.min_tableid = file_meta->delta_l0_meta.tableid_min;
        recovered.max_tableid = file_meta->delta_l0_meta.tableid_max;
        recovered.observed_tableids.insert(file_meta->delta_l0_meta.tableid_min);
        recovered.observed_tableids.insert(file_meta->delta_l0_meta.tableid_max);
        grouped.emplace(key, std::move(recovered));
      } else {
        iter->second.min_tableid =
            std::min(iter->second.min_tableid,
                     file_meta->delta_l0_meta.tableid_min);
        iter->second.max_tableid =
            std::max(iter->second.max_tableid,
                     file_meta->delta_l0_meta.tableid_max);
        iter->second.observed_tableids.insert(
            file_meta->delta_l0_meta.tableid_min);
        iter->second.observed_tableids.insert(
            file_meta->delta_l0_meta.tableid_max);
      }
    }

    if (grouped.empty()) {
      ResetToSinglePartition();
      return;
    }

    std::vector<RecoveredPartition> candidates;
    candidates.reserve(grouped.size());
    for (const auto& entry : grouped) {
      candidates.push_back(entry.second);
    }
    std::sort(
        candidates.begin(), candidates.end(),
        [](const RecoveredPartition& lhs, const RecoveredPartition& rhs) {
          if (lhs.generation != rhs.generation) {
            return lhs.generation > rhs.generation;
          }
          const uint64_t lhs_width = lhs.max_tableid - lhs.min_tableid;
          const uint64_t rhs_width = rhs.max_tableid - rhs.min_tableid;
          if (lhs_width != rhs_width) {
            return lhs_width < rhs_width;
          }
          if (lhs.min_tableid != rhs.min_tableid) {
            return lhs.min_tableid < rhs.min_tableid;
          }
          return lhs.partition_id < rhs.partition_id;
        });

    std::vector<RecoveredPartition> selected;
    for (const RecoveredPartition& candidate : candidates) {
      bool overlaps = false;
      for (const RecoveredPartition& existing : selected) {
        if (!(candidate.max_tableid < existing.min_tableid ||
              candidate.min_tableid > existing.max_tableid)) {
          overlaps = true;
          break;
        }
      }
      if (!overlaps) {
        selected.push_back(candidate);
      }
    }

    if (selected.empty()) {
      ResetToSinglePartition();
      return;
    }

    std::sort(selected.begin(), selected.end(),
              [](const RecoveredPartition& lhs,
                 const RecoveredPartition& rhs) {
                if (lhs.min_tableid != rhs.min_tableid) {
                  return lhs.min_tableid < rhs.min_tableid;
                }
                return lhs.partition_id < rhs.partition_id;
              });

    std::vector<DeltaL0Partition> restored;
    restored.reserve(selected.size());
    uint64_t max_partition_id = 0;
    for (size_t index = 0; index < selected.size(); ++index) {
      const RecoveredPartition& recovered = selected[index];
      DeltaL0Partition partition;
      partition.partition_id = recovered.partition_id;
      partition.generation = recovered.generation;
      partition.tableid_begin = (index == 0) ? 0 : recovered.min_tableid;
      partition.tableid_end =
          (index + 1 < selected.size())
              ? selected[index + 1].min_tableid - 1
              : std::numeric_limits<uint64_t>::max();
      for (uint64_t observed_tableid : recovered.observed_tableids) {
        if (partition.Contains(observed_tableid)) {
          partition.observed_tableids.insert(observed_tableid);
        }
      }
      restored.push_back(std::move(partition));
      max_partition_id = std::max(max_partition_id, recovered.partition_id);
    }

    if (!ValidatePartitions(restored, max_partition_id + 1)) {
      ResetToSinglePartition();
      return;
    }

    partitions_ = std::move(restored);
    next_partition_id_ = max_partition_id + 1;
    split_count_ = partitions_.empty() ? 0 : partitions_.size() - 1;
  }

  // 记录 Get 路径中 L0 候选文件剪枝前后的数量。
  // 使用 relaxed 原子累加，因为这里只用于观测统计，不参与同步控制。
  void RecordGetL0CandidateFiles(uint64_t before_prune,
                                 uint64_t after_prune) const {
    read_prune_counters_.get_l0_candidate_files_before_prune.fetch_add(
        before_prune, std::memory_order_relaxed);
    read_prune_counters_.get_l0_candidate_files_after_prune.fetch_add(
        after_prune, std::memory_order_relaxed);
  }

  // 记录 Scan 路径中 L0 候选文件剪枝前后的数量。
  // 使用 relaxed 原子累加，因为这里只用于观测统计，不参与同步控制。
  void RecordScanL0CandidateFiles(uint64_t before_prune,
                                  uint64_t after_prune) const {
    read_prune_counters_.scan_l0_candidate_files_before_prune.fetch_add(
        before_prune, std::memory_order_relaxed);
    read_prune_counters_.scan_l0_candidate_files_after_prune.fetch_add(
        after_prune, std::memory_order_relaxed);
  }

  // 读取并返回当前读剪枝统计计数器快照。
  // 每个计数以 relaxed load 读取，适合观测用途。
  DeltaL0ReadPruneCountersSnapshot GetReadPruneCountersSnapshot() const {
    DeltaL0ReadPruneCountersSnapshot snapshot;
    snapshot.get_l0_candidate_files_before_prune =
        read_prune_counters_.get_l0_candidate_files_before_prune.load(
            std::memory_order_relaxed);
    snapshot.get_l0_candidate_files_after_prune =
        read_prune_counters_.get_l0_candidate_files_after_prune.load(
            std::memory_order_relaxed);
    snapshot.scan_l0_candidate_files_before_prune =
        read_prune_counters_.scan_l0_candidate_files_before_prune.load(
            std::memory_order_relaxed);
    snapshot.scan_l0_candidate_files_after_prune =
        read_prune_counters_.scan_l0_candidate_files_after_prune.load(
            std::memory_order_relaxed);
    return snapshot;
  }

 private:
  // 将目录重置为初始状态：一个覆盖全 table id 空间的分区。
  // 同时重置 next_partition_id 和 split_count。
  void ResetToSinglePartition() {
    partitions_.clear();
    partitions_.push_back(DeltaL0Partition{1, 0, 0,
                                           std::numeric_limits<uint64_t>::max(),
                                           {}});
    next_partition_id_ = 2;
    split_count_ = 0;
  }

  // 校验分区列表是否构成合法的连续覆盖区间。
  // 要求分区非空、ID 非 0、区间从 0 开始连续衔接、最后一个分区结束于 uint64_t 最大值，且观察值都落在所属分区内。
  static bool ValidatePartitions(const std::vector<DeltaL0Partition>& partitions,
                                 uint64_t next_partition_id) {
    if (partitions.empty()) {
      return false;
    }

    uint64_t max_partition_id = 0;
    uint64_t expected_begin = 0;
    for (size_t index = 0; index < partitions.size(); ++index) {
      const DeltaL0Partition& partition = partitions[index];
      if (partition.partition_id == 0 ||
          partition.tableid_begin != expected_begin ||
          partition.tableid_begin > partition.tableid_end) {
        return false;
      }
      for (uint64_t observed_tableid : partition.observed_tableids) {
        if (!partition.Contains(observed_tableid)) {
          return false;
        }
      }
      max_partition_id = std::max(max_partition_id, partition.partition_id);
      if (index + 1 == partitions.size()) {
        if (partition.tableid_end != std::numeric_limits<uint64_t>::max()) {
          return false;
        }
      } else {
        if (partition.tableid_end == std::numeric_limits<uint64_t>::max()) {
          return false;
        }
        expected_begin = partition.tableid_end + 1;
      }
    }
    return next_partition_id > max_partition_id;
  }

  // 使用二分查找定位指定 table id 所属分区在 partitions_ 中的下标。
  // partitions_ 按 tableid_end 递增排列。
  size_t FindPartitionIndex(uint64_t tableid) const {
    auto it = std::lower_bound(
        partitions_.begin(), partitions_.end(), tableid,
        [](const DeltaL0Partition& partition, uint64_t value) {
          return partition.tableid_end < value;
        });
    if (it == partitions_.end()) {
      return partitions_.size() - 1;
    }
    return static_cast<size_t>(it - partitions_.begin());
  }

  // 判断指定分区是否满足拆分条件。
  // 需要未超过最大分区数、观察到足够多 table id，并且超过目标分区大小的拆分阈值。
  bool ShouldSplit(const DeltaL0Partition& partition) const {
    return partitions_.size() < kMaxPartitionCount &&
           partition.observed_tableids.size() >= kMinTablesToSplit &&
           partition.observed_tableids.size() >
               kTargetTablesPerPartition * kSplitFactor;
  }

  // 在指定分区满足条件时执行一次拆分。
  // 拆分点取观察到的 table id 的中位位置，生成左右两个新分区并更新 split_count。
  void MaybeSplit(size_t index) {
    const DeltaL0Partition& partition = partitions_[index];
    if (!ShouldSplit(partition)) {
      return;
    }

    const size_t split_pos = partition.observed_tableids.size() / 2;
    auto right_it = partition.observed_tableids.begin();
    std::advance(right_it, static_cast<long>(split_pos));
    if (right_it == partition.observed_tableids.begin() ||
        right_it == partition.observed_tableids.end()) {
      return;
    }

    const uint64_t right_begin = *right_it;
    if (right_begin == 0 || right_begin <= partition.tableid_begin) {
      return;
    }

    DeltaL0Partition left;
    left.partition_id = next_partition_id_++;
    left.generation = partition.generation + 1;
    left.tableid_begin = partition.tableid_begin;
    left.tableid_end = right_begin - 1;

    DeltaL0Partition right;
    right.partition_id = next_partition_id_++;
    right.generation = partition.generation + 1;
    right.tableid_begin = right_begin;
    right.tableid_end = partition.tableid_end;

    for (uint64_t observed_tableid : partition.observed_tableids) {
      if (observed_tableid < right_begin) {
        left.observed_tableids.insert(observed_tableid);
      } else {
        right.observed_tableids.insert(observed_tableid);
      }
    }

    partitions_[index] = std::move(left);
    partitions_.insert(partitions_.begin() + static_cast<long>(index + 1),
                       std::move(right));
    ++split_count_;
  }

  std::vector<DeltaL0Partition> partitions_;
  uint64_t next_partition_id_ = 1;
  uint64_t split_count_ = 0;
  mutable DeltaL0ReadPruneCounters read_prune_counters_;
};

// 判断文件元信息中是否带有有效的 Delta L0 分区身份。
// 有效身份要求 delta_l0_meta.valid 为 true 且 partition_id 非 0。
inline bool DeltaL0FileHasPartitionIdentity(const FileMetaData& file_meta) {
  return file_meta.delta_l0_meta.valid &&
         file_meta.delta_l0_meta.partition_id != 0;
}

// 判断文件是否属于缺少 Delta L0 元信息的旧格式文件。
// 这类文件通常需要在读/剪枝路径上保守处理。
inline bool DeltaL0FileIsLegacyWithoutMeta(const FileMetaData& file_meta) {
  return !file_meta.delta_l0_meta.valid;
}

// 判断文件是否来自当前分区目录中已经过期的父分区。
// 若文件记录的分区身份与当前 table id 所属分区不一致，或 generation 落后，则视为旧父分区文件。
inline bool DeltaL0FileIsOldParent(
    const FileMetaData& file_meta,
    const DeltaL0PartitionDirectory& directory) {
  if (!DeltaL0FileHasPartitionIdentity(file_meta)) {
    return false;
  }
  const DeltaL0Partition& current_min =
      directory.Lookup(file_meta.delta_l0_meta.tableid_min);
  const DeltaL0Partition& current_max =
      directory.Lookup(file_meta.delta_l0_meta.tableid_max);
  if (current_min.partition_id != current_max.partition_id ||
      current_min.generation != current_max.generation) {
    return true;
  }
  return file_meta.delta_l0_meta.partition_id != current_min.partition_id ||
         file_meta.delta_l0_meta.partition_generation < current_min.generation;
}

}  // namespace ROCKSDB_NAMESPACE
