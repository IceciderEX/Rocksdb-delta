// DELTA_PARTITION_OBSERVABILITY_OR_TEST_ONLY: ad-hoc benchmark helper for
// evaluating Delta L0 partition effectiveness under a Delta-like lifecycle.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "db/delta_l0.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"

namespace ROCKSDB_NAMESPACE {
namespace {

struct BenchOptions {
  std::string db_root = "db_perf_test/delta_partition_bench";
  std::string mode = "baseline";
  double target_seconds = 6.0;
  uint64_t min_passes = 1;
  uint64_t num_tables = 96;
  uint64_t cuids_per_table = 2;
  uint64_t insert_rows_per_cuid = 64;
  uint64_t insert_chunk_rows = 8;
  uint64_t update_merge_rounds = 2;
  uint64_t updates_per_merge = 96;
  uint64_t update_chunk_ops = 8;
  uint64_t user_scans_per_insert_chunk = 1;
  uint64_t user_scans_per_update_chunk = 2;
  uint64_t range_scan_len = 8;
  uint64_t value_size = 128;
  uint64_t seed = 7;
};

struct MetricCounter {
  uint64_t ops = 0;
  uint64_t rows = 0;
  double seconds = 0;
};

struct BenchStats {
  MetricCounter insert_puts;
  MetricCounter update_puts;
  MetricCounter merge_deletes;
  MetricCounter point_gets;
  MetricCounter user_scans;
  MetricCounter merge_scans;
  uint64_t point_get_hits = 0;
  uint64_t point_get_misses = 0;
  uint64_t insert_merge_count = 0;
  uint64_t update_merge_count = 0;
  uint64_t completed_passes = 0;
  uint64_t processed_cuids = 0;
};

void AppendBigEndian64(std::string* dst, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    dst->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

void AppendBigEndian32(std::string* dst, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    dst->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

std::string DeltaKey(uint64_t tableid, uint64_t cuid, uint64_t rowid,
                     uint64_t event_seq = 1) {
  std::string key;
  key.reserve(kDeltaBinaryKeySize);
  AppendBigEndian32(&key, 1);
  AppendBigEndian32(&key, static_cast<uint32_t>(tableid));
  AppendBigEndian64(&key, cuid);
  AppendBigEndian32(&key, static_cast<uint32_t>(rowid));
  AppendBigEndian64(&key, event_seq);
  return key;
}

std::string DeltaCuidLowerBound(uint64_t tableid, uint64_t cuid) {
  return DeltaKey(tableid, cuid, 0, 0);
}

std::string DeltaCuidUpperBound(uint64_t tableid, uint64_t cuid) {
  return DeltaKey(tableid, cuid + 1, 0, 0);
}

void RequireOk(const Status& status, const std::string& step) {
  if (!status.ok()) {
    std::cerr << "step=" << step << " status=" << status.ToString() << "\n";
    std::exit(1);
  }
}

BenchOptions ParseArgs(int argc, char** argv) {
  BenchOptions options;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    const auto pos = arg.find('=');
    if (pos == std::string::npos || arg.rfind("--", 0) != 0) {
      continue;
    }
    const std::string key = arg.substr(2, pos - 2);
    const std::string value = arg.substr(pos + 1);
    if (key == "db_root") {
      options.db_root = value;
    } else if (key == "mode") {
      options.mode = value;
    } else if (key == "target_seconds") {
      options.target_seconds = std::stod(value);
    } else if (key == "min_passes") {
      options.min_passes = std::stoull(value);
    } else if (key == "num_tables") {
      options.num_tables = std::stoull(value);
    } else if (key == "cuids_per_table") {
      options.cuids_per_table = std::stoull(value);
    } else if (key == "insert_rows_per_cuid") {
      options.insert_rows_per_cuid = std::stoull(value);
    } else if (key == "insert_chunk_rows") {
      options.insert_chunk_rows = std::stoull(value);
    } else if (key == "update_merge_rounds") {
      options.update_merge_rounds = std::stoull(value);
    } else if (key == "updates_per_merge") {
      options.updates_per_merge = std::stoull(value);
    } else if (key == "update_chunk_ops") {
      options.update_chunk_ops = std::stoull(value);
    } else if (key == "user_scans_per_insert_chunk") {
      options.user_scans_per_insert_chunk = std::stoull(value);
    } else if (key == "user_scans_per_update_chunk") {
      options.user_scans_per_update_chunk = std::stoull(value);
    } else if (key == "range_scan_len") {
      options.range_scan_len = std::stoull(value);
    } else if (key == "value_size") {
      options.value_size = std::stoull(value);
    } else if (key == "seed") {
      options.seed = std::stoull(value);
    }
  }
  options.target_seconds = std::max(1.0, options.target_seconds);
  options.min_passes = std::max<uint64_t>(1, options.min_passes);
  options.num_tables = std::max<uint64_t>(1, options.num_tables);
  options.cuids_per_table = std::max<uint64_t>(1, options.cuids_per_table);
  options.insert_rows_per_cuid =
      std::max<uint64_t>(1, options.insert_rows_per_cuid);
  options.insert_chunk_rows = std::max<uint64_t>(1, options.insert_chunk_rows);
  options.update_merge_rounds =
      std::max<uint64_t>(1, options.update_merge_rounds);
  options.updates_per_merge = std::max<uint64_t>(1, options.updates_per_merge);
  options.update_chunk_ops = std::max<uint64_t>(1, options.update_chunk_ops);
  options.range_scan_len = std::max<uint64_t>(1, options.range_scan_len);
  return options;
}

Options BuildOptions() {
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.compression = kNoCompression;
  options.write_buffer_size = 2 << 20;
  options.target_file_size_base = 2 << 20;
  options.max_open_files = -1;
  options.level0_file_num_compaction_trigger = 8;
  options.disable_auto_compactions = true;
  options.max_background_jobs = 2;

  BlockBasedTableOptions table_options;
  table_options.block_size = 16 * 1024;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  return options;
}

struct DbHolder {
  DB* db = nullptr;
  ColumnFamilyHandle* handle = nullptr;
  std::vector<ColumnFamilyHandle*> handles;

  DbHolder() = default;
  DbHolder(const DbHolder&) = delete;
  DbHolder& operator=(const DbHolder&) = delete;
  DbHolder(DbHolder&& other) noexcept
      : db(other.db), handle(other.handle), handles(std::move(other.handles)) {
    other.db = nullptr;
    other.handle = nullptr;
  }
  DbHolder& operator=(DbHolder&& other) noexcept {
    if (this != &other) {
      db = other.db;
      handle = other.handle;
      handles = std::move(other.handles);
      other.db = nullptr;
      other.handle = nullptr;
    }
    return *this;
  }

  ~DbHolder() {
    if (db == nullptr) {
      return;
    }
    for (ColumnFamilyHandle* cfh : handles) {
      if (cfh != nullptr) {
        RequireOk(db->DestroyColumnFamilyHandle(cfh),
                  "DestroyColumnFamilyHandle");
      }
    }
    delete db;
  }
};

DbHolder OpenDb(const BenchOptions& bench_options, const Options& options,
                const std::string& db_path) {
  DestroyDB(db_path, options);

  DbHolder holder;
  if (bench_options.mode == "delta") {
    std::vector<ColumnFamilyDescriptor> cf_descriptors = {
        ColumnFamilyDescriptor(kDefaultColumnFamilyName,
                               ColumnFamilyOptions(options)),
        ColumnFamilyDescriptor("Delta", ColumnFamilyOptions(options))};
    RequireOk(DB::Open(DBOptions(options), db_path, cf_descriptors,
                       &holder.handles, &holder.db),
              "OpenDeltaDb");
    holder.handle = holder.handles[1];
  } else {
    RequireOk(DB::Open(options, db_path, &holder.db), "OpenBaselineDb");
    holder.handle = holder.db->DefaultColumnFamily();
  }
  return holder;
}

double SecondsSince(
    const std::chrono::steady_clock::time_point& start_time) {
  return std::chrono::duration_cast<std::chrono::duration<double>>(
             std::chrono::steady_clock::now() - start_time)
      .count();
}

double Qps(uint64_t ops, double seconds) {
  return static_cast<double>(ops) / std::max(0.000001, seconds);
}

void AppendMetric(MetricCounter* metric, uint64_t ops, uint64_t rows,
                  double seconds) {
  metric->ops += ops;
  metric->rows += rows;
  metric->seconds += seconds;
}

void WriteAndFlush(DB* db, ColumnFamilyHandle* handle, WriteBatch* batch,
                   FlushOptions* flush_options, MetricCounter* metric,
                   uint64_t op_count, const std::string& step) {
  if (op_count == 0) {
    return;
  }
  WriteOptions write_options;
  write_options.disableWAL = true;
  const auto start = std::chrono::steady_clock::now();
  RequireOk(db->Write(write_options, batch), step + "_Write");
  RequireOk(db->Flush(*flush_options, handle), step + "_Flush");
  AppendMetric(metric, op_count, 0, SecondsSince(start));
}

std::string BuildValue(char tag, uint64_t ordinal, uint64_t value_size) {
  std::string value(value_size, tag);
  const std::string suffix = "_" + std::to_string(ordinal);
  if (suffix.size() <= value.size()) {
    value.replace(value.size() - suffix.size(), suffix.size(), suffix);
  }
  return value;
}

void RunUserRangeScans(DB* db, ColumnFamilyHandle* handle,
                       const BenchOptions& bench_options, uint64_t cuid,
                       uint64_t visible_rows, uint64_t scans_per_chunk,
                       std::mt19937_64* rng, BenchStats* stats) {
  if (visible_rows == 0 || scans_per_chunk == 0) {
    return;
  }
  std::uniform_int_distribution<uint64_t> table_dist(1, bench_options.num_tables);
  const uint64_t clamped_visible_rows =
      std::min<uint64_t>(visible_rows, bench_options.insert_rows_per_cuid);
  const uint64_t max_start =
      clamped_visible_rows > bench_options.range_scan_len
          ? clamped_visible_rows - bench_options.range_scan_len
          : 0;
  std::uniform_int_distribution<uint64_t> start_dist(0, max_start);
  for (uint64_t i = 0; i < scans_per_chunk; ++i) {
    const uint64_t tableid = table_dist(*rng);
    const uint64_t start_row = start_dist(*rng);
    const uint64_t end_row =
        std::min<uint64_t>(bench_options.insert_rows_per_cuid,
                           start_row + bench_options.range_scan_len);
    const std::string lower = DeltaKey(tableid, cuid, start_row, 0);
    const std::string upper = DeltaKey(tableid, cuid, end_row, 0);
    const Slice lower_slice(lower);
    const Slice upper_slice(upper);
    ReadOptions read_options;
    read_options.iterate_lower_bound = &lower_slice;
    read_options.iterate_upper_bound = &upper_slice;
    const auto start = std::chrono::steady_clock::now();
    std::unique_ptr<Iterator> iter(db->NewIterator(read_options, handle));
    uint64_t rows_read = 0;
    for (iter->Seek(lower);
         iter->Valid() && rows_read < bench_options.range_scan_len;
         iter->Next()) {
      ++rows_read;
    }
    if (!iter->status().ok()) {
      RequireOk(iter->status(), "UserRangeScan");
    }
    AppendMetric(&stats->user_scans, 1, rows_read, SecondsSince(start));
  }
}

void RunPointLookup(DB* db, ColumnFamilyHandle* handle, uint64_t tableid,
                    uint64_t cuid, uint64_t rowid, BenchStats* stats) {
  ReadOptions read_options;
  PinnableSlice value;
  const std::string key = DeltaKey(tableid, cuid, rowid);
  const auto start = std::chrono::steady_clock::now();
  const Status status = db->Get(read_options, handle, key, &value);
  const double seconds = SecondsSince(start);
  if (status.ok()) {
    ++stats->point_get_hits;
  } else if (status.IsNotFound()) {
    ++stats->point_get_misses;
  } else {
    RequireOk(status, "UpdatePointLookup");
  }
  AppendMetric(&stats->point_gets, 1, 0, seconds);
}

std::vector<std::string> CollectVisibleKeysForCuid(
    DB* db, ColumnFamilyHandle* handle, uint64_t tableid, uint64_t cuid,
    BenchStats* stats) {
  const std::string lower = DeltaCuidLowerBound(tableid, cuid);
  const std::string upper = DeltaCuidUpperBound(tableid, cuid);
  const Slice lower_slice(lower);
  const Slice upper_slice(upper);
  ReadOptions read_options;
  read_options.iterate_lower_bound = &lower_slice;
  read_options.iterate_upper_bound = &upper_slice;

  const auto start = std::chrono::steady_clock::now();
  std::unique_ptr<Iterator> iter(db->NewIterator(read_options, handle));
  std::vector<std::string> keys;
  for (iter->Seek(lower); iter->Valid(); iter->Next()) {
    keys.emplace_back(iter->key().ToString());
  }
  if (!iter->status().ok()) {
    RequireOk(iter->status(), "MergeScan");
  }
  AppendMetric(&stats->merge_scans, 1, keys.size(), SecondsSince(start));
  return keys;
}

void DeleteKeys(DB* db, ColumnFamilyHandle* handle,
                const std::vector<std::string>& keys,
                const BenchOptions& bench_options, FlushOptions* flush_options,
                BenchStats* stats, const std::string& merge_step) {
  if (keys.empty()) {
    return;
  }
  WriteBatch batch;
  uint64_t op_count = 0;
  const uint64_t batch_limit =
      std::max<uint64_t>(bench_options.insert_chunk_rows *
                             bench_options.num_tables,
                         1024);
  for (const std::string& key : keys) {
    RequireOk(batch.Delete(handle, key), merge_step + "_Delete");
    ++op_count;
    if (op_count >= batch_limit) {
      WriteAndFlush(db, handle, &batch, flush_options, &stats->merge_deletes,
                    op_count, merge_step);
      batch.Clear();
      op_count = 0;
    }
  }
  if (op_count > 0) {
    WriteAndFlush(db, handle, &batch, flush_options, &stats->merge_deletes,
                  op_count, merge_step);
  }
}

void ExecuteInsertLifecycle(DB* db, ColumnFamilyHandle* handle,
                            const BenchOptions& bench_options,
                            uint64_t cuid_begin, uint64_t cuid_end,
                            FlushOptions* flush_options, std::mt19937_64* rng,
                            BenchStats* stats) {
  uint64_t value_ordinal = 1;
  for (uint64_t cuid = cuid_begin; cuid <= cuid_end; ++cuid) {
    for (uint64_t row_base = 0; row_base < bench_options.insert_rows_per_cuid;
         row_base += bench_options.insert_chunk_rows) {
      WriteBatch batch;
      uint64_t put_count = 0;
      const uint64_t row_end = std::min<uint64_t>(
          bench_options.insert_rows_per_cuid,
          row_base + bench_options.insert_chunk_rows);
      for (uint64_t tableid = 1; tableid <= bench_options.num_tables; ++tableid) {
        for (uint64_t rowid = row_base; rowid < row_end; ++rowid) {
          RequireOk(batch.Put(handle, DeltaKey(tableid, cuid, rowid),
                              BuildValue('i', value_ordinal++,
                                         bench_options.value_size)),
                    "InsertPut");
          ++put_count;
        }
      }
      WriteAndFlush(db, handle, &batch, flush_options, &stats->insert_puts,
                    put_count, "InsertChunk");
      RunUserRangeScans(db, handle, bench_options, cuid, row_end,
                        bench_options.user_scans_per_insert_chunk, rng, stats);
    }

    for (uint64_t tableid = 1; tableid <= bench_options.num_tables; ++tableid) {
      std::vector<std::string> keys =
          CollectVisibleKeysForCuid(db, handle, tableid, cuid, stats);
      DeleteKeys(db, handle, keys, bench_options, flush_options, stats,
                 "InsertMerge");
      ++stats->insert_merge_count;
    }
  }
}

void ExecuteUpdateLifecycle(DB* db, ColumnFamilyHandle* handle,
                            const BenchOptions& bench_options,
                            uint64_t cuid_begin, uint64_t cuid_end,
                            FlushOptions* flush_options, std::mt19937_64* rng,
                            BenchStats* stats) {
  uint64_t value_ordinal = 1;
  for (uint64_t cuid = cuid_begin; cuid <= cuid_end; ++cuid) {
    for (uint64_t merge_round = 0;
         merge_round < bench_options.update_merge_rounds; ++merge_round) {
      for (uint64_t update_base = 0; update_base < bench_options.updates_per_merge;
           update_base += bench_options.update_chunk_ops) {
        WriteBatch batch;
        uint64_t put_count = 0;
        const uint64_t update_end = std::min<uint64_t>(
            bench_options.updates_per_merge,
            update_base + bench_options.update_chunk_ops);
        for (uint64_t tableid = 1; tableid <= bench_options.num_tables;
             ++tableid) {
          for (uint64_t op_index = update_base; op_index < update_end;
               ++op_index) {
            const uint64_t rowid = op_index % bench_options.insert_rows_per_cuid;
            RunPointLookup(db, handle, tableid, cuid, rowid, stats);
            RequireOk(batch.Put(handle, DeltaKey(tableid, cuid, rowid),
                                BuildValue('u', value_ordinal++,
                                           bench_options.value_size)),
                      "UpdatePut");
            ++put_count;
          }
        }
        WriteAndFlush(db, handle, &batch, flush_options, &stats->update_puts,
                      put_count, "UpdateChunk");
        const uint64_t visible_rows =
            std::min<uint64_t>(bench_options.insert_rows_per_cuid, update_end);
        RunUserRangeScans(db, handle, bench_options, cuid, visible_rows,
                          bench_options.user_scans_per_update_chunk, rng, stats);
      }

      for (uint64_t tableid = 1; tableid <= bench_options.num_tables; ++tableid) {
        std::vector<std::string> keys =
            CollectVisibleKeysForCuid(db, handle, tableid, cuid, stats);
        DeleteKeys(db, handle, keys, bench_options, flush_options, stats,
                   "UpdateMerge");
        ++stats->update_merge_count;
      }
    }
  }
}

void RunLifecyclePass(DB* db, ColumnFamilyHandle* handle,
                      const BenchOptions& bench_options, uint64_t cuid,
                      FlushOptions* flush_options, std::mt19937_64* rng,
                      BenchStats* stats) {
  ExecuteInsertLifecycle(db, handle, bench_options, cuid, cuid,
                         flush_options, rng, stats);
  ExecuteUpdateLifecycle(db, handle, bench_options, cuid, cuid,
                         flush_options, rng, stats);
  ++stats->completed_passes;
  ++stats->processed_cuids;
}

void PrintDeltaStats(DB* db, ColumnFamilyHandle* handle) {
  std::map<std::string, std::string> delta_stats;
  if (!db->GetMapProperty(handle, DB::Properties::kDeltaL0Stats,
                          &delta_stats)) {
    std::cout << "delta_l0_stats_available=0\n";
    return;
  }
  std::cout << "delta_l0_stats_available=1\n";
  for (const auto& entry : delta_stats) {
    std::cout << "delta_l0_stats." << entry.first << "=" << entry.second
              << "\n";
  }
}

void PrintBenchStats(const BenchOptions& bench_options, const BenchStats& stats,
                     const std::string& db_path, double total_seconds,
                     DB* db, ColumnFamilyHandle* handle) {
  std::string num_l0_files;
  db->GetProperty(handle, DB::Properties::kNumFilesAtLevelPrefix + "0",
                  &num_l0_files);

  const uint64_t total_write_ops = stats.insert_puts.ops + stats.update_puts.ops +
                                   stats.merge_deletes.ops;
  const double total_write_seconds = stats.insert_puts.seconds +
                                     stats.update_puts.seconds +
                                     stats.merge_deletes.seconds;

  std::cout << "mode=" << bench_options.mode << "\n";
  std::cout << "db_path=" << db_path << "\n";
  std::cout << "target_seconds=" << bench_options.target_seconds << "\n";
  std::cout << "min_passes=" << bench_options.min_passes << "\n";
  std::cout << "completed_passes=" << stats.completed_passes << "\n";
  std::cout << "processed_cuids=" << stats.processed_cuids << "\n";
  std::cout << "num_tables=" << bench_options.num_tables << "\n";
  std::cout << "cuids_per_table=" << bench_options.cuids_per_table << "\n";
  std::cout << "insert_rows_per_cuid=" << bench_options.insert_rows_per_cuid
            << "\n";
  std::cout << "update_merge_rounds=" << bench_options.update_merge_rounds
            << "\n";
  std::cout << "updates_per_merge=" << bench_options.updates_per_merge << "\n";
  std::cout << "total_seconds=" << total_seconds << "\n";
  std::cout << "write_ops=" << total_write_ops << "\n";
  std::cout << "write_seconds=" << total_write_seconds << "\n";
  std::cout << "write_qps=" << Qps(total_write_ops, total_write_seconds)
            << "\n";
  std::cout << "insert_put_ops=" << stats.insert_puts.ops << "\n";
  std::cout << "insert_put_seconds=" << stats.insert_puts.seconds << "\n";
  std::cout << "insert_put_qps="
            << Qps(stats.insert_puts.ops, stats.insert_puts.seconds) << "\n";
  std::cout << "update_put_ops=" << stats.update_puts.ops << "\n";
  std::cout << "update_put_seconds=" << stats.update_puts.seconds << "\n";
  std::cout << "update_put_qps="
            << Qps(stats.update_puts.ops, stats.update_puts.seconds) << "\n";
  std::cout << "merge_delete_ops=" << stats.merge_deletes.ops << "\n";
  std::cout << "merge_delete_seconds=" << stats.merge_deletes.seconds << "\n";
  std::cout << "merge_delete_qps="
            << Qps(stats.merge_deletes.ops, stats.merge_deletes.seconds)
            << "\n";
  std::cout << "point_get_ops=" << stats.point_gets.ops << "\n";
  std::cout << "point_get_seconds=" << stats.point_gets.seconds << "\n";
  std::cout << "point_get_qps="
            << Qps(stats.point_gets.ops, stats.point_gets.seconds) << "\n";
  std::cout << "point_get_hits=" << stats.point_get_hits << "\n";
  std::cout << "point_get_misses=" << stats.point_get_misses << "\n";
  std::cout << "user_scan_ops=" << stats.user_scans.ops << "\n";
  std::cout << "user_scan_rows=" << stats.user_scans.rows << "\n";
  std::cout << "user_scan_seconds=" << stats.user_scans.seconds << "\n";
  std::cout << "user_scan_qps="
            << Qps(stats.user_scans.ops, stats.user_scans.seconds) << "\n";
  std::cout << "merge_scan_ops=" << stats.merge_scans.ops << "\n";
  std::cout << "merge_scan_rows=" << stats.merge_scans.rows << "\n";
  std::cout << "merge_scan_seconds=" << stats.merge_scans.seconds << "\n";
  std::cout << "merge_scan_qps="
            << Qps(stats.merge_scans.ops, stats.merge_scans.seconds) << "\n";
  std::cout << "insert_merge_count=" << stats.insert_merge_count << "\n";
  std::cout << "update_merge_count=" << stats.update_merge_count << "\n";
  std::cout << "num_files_at_level0=" << num_l0_files << "\n";

  if (bench_options.mode == "delta") {
    PrintDeltaStats(db, handle);
  }
}

}  // namespace

int DeltaPartitionBenchMain(int argc, char** argv) {
  const BenchOptions bench_options = ParseArgs(argc, argv);
  const Options options = BuildOptions();
  const std::string db_path =
      bench_options.db_root + "/" + bench_options.mode + "_db";

  std::filesystem::create_directories(bench_options.db_root);
  DbHolder db_holder = OpenDb(bench_options, options, db_path);
  FlushOptions flush_options;
  flush_options.wait = true;

  BenchStats stats;
  std::mt19937_64 rng(bench_options.seed);
  const auto total_start = std::chrono::steady_clock::now();
  uint64_t next_cuid = 1;
  while (stats.completed_passes < bench_options.min_passes ||
         SecondsSince(total_start) < bench_options.target_seconds) {
    for (uint64_t offset = 0; offset < bench_options.cuids_per_table; ++offset) {
      RunLifecyclePass(db_holder.db, db_holder.handle, bench_options, next_cuid,
                       &flush_options, &rng, &stats);
      ++next_cuid;
      if (stats.completed_passes >= bench_options.min_passes &&
          SecondsSince(total_start) >= bench_options.target_seconds) {
        break;
      }
    }
  }
  const double total_seconds = SecondsSince(total_start);

  PrintBenchStats(bench_options, stats, db_path, total_seconds, db_holder.db,
                  db_holder.handle);
  return 0;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  return ROCKSDB_NAMESPACE::DeltaPartitionBenchMain(argc, argv);
}
