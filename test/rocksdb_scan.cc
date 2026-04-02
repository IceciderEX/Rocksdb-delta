/**
 * rocksdb_perf_test.cc
 *
 * RocksDB-delta 高性能并发测试工具 (重构版 - 顺序生命周期模型)
 * 场景：16线程并发, 8分钟持续运行, 10W CUID, 15/77 负载分布
 * 逻辑：每线程领取一个 CUID 走完“增删改查”一生，所有 CUID 行数保持一致 (~2.5W行)。
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <filesystem>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"

// ==========================================
// 配置常量
// ==========================================
const std::string kNativeDBPath = "/home/jx/Rocksdb-delta/db_perf_test/db_perf_native";
const std::string kDeltaDBPath = "/home/jx/Rocksdb-delta/db_perf_test/db_perf_delta";
const int kNumThreads = 16;
const int kDefaultTestDurationSec = 6000;       // 10 分钟
const int kNumTableIds = 64;            // 固定 64 个 tableid
const int kActiveTableWindow = 16;      // 同一时段活跃 tableid 数量
const int kCuidsPerTable = 1600;        // 每个 tableid 的 CUID 数量一致
const int kNumCuids = kNumTableIds * kCuidsPerTable;
const int kBatchSize = 128;             // 每次 Put 128 行
const int kTargetPutBatches = 200;      // 每个 CUID 固定写入 200 个 batch (约 25,600 行)
const double kHotRatio = 0.15;          // 15% 的热点
const int kHotScanTarget = 1000;        // 热点访问目标
const int kColdScanTarget = 150;        // 普通访问目标
const uint64_t kDbId = 1;

// ==========================================
// 辅助工具与状态管理
// ==========================================

bool CleanupDBPath(const std::string& path) {
  std::error_code ec;
  if (path != kNativeDBPath && path != kDeltaDBPath) {
    std::cerr << "Refusing to delete non-test path: " << path << std::endl;
    return false;
  }
  std::filesystem::remove_all(path, ec);
  return true;
}

void EncodeUint64BE(uint64_t value, char* out) {
  for (int i = 0; i < 8; ++i) {
    out[i] = static_cast<char>((value >> (56 - 8 * i)) & 0xFF);
  }
}

uint64_t TableIdFromCUID(uint64_t cuid) {
  const uint64_t max_table_id = static_cast<uint64_t>(kNumTableIds - 1);
  const uint64_t table_id =
      cuid / static_cast<uint64_t>(kCuidsPerTable);
  return std::min<uint64_t>(table_id, max_table_id);
}

int32_t PartitionIdFromTableId(uint64_t table_id) {
  return static_cast<int32_t>(table_id % 16);
}

uint64_t LogicalIndexToCUID(uint64_t logical_idx) {
  const uint64_t tables_per_window = static_cast<uint64_t>(kActiveTableWindow);
  const uint64_t window_size = tables_per_window *
                               static_cast<uint64_t>(kCuidsPerTable);
  const uint64_t window_id = logical_idx / window_size;
  const uint64_t idx_in_window = logical_idx % window_size;
  const uint64_t table_slot = idx_in_window % tables_per_window;
  const uint64_t offset = idx_in_window / tables_per_window;
  const uint64_t table_id = window_id * tables_per_window + table_slot;
  return table_id * static_cast<uint64_t>(kCuidsPerTable) + offset;
}

bool IsHotCUIDUniformAcrossTables(uint64_t cuid) {
  // Make hot CUID distribution depend on per-table offset only,
  // so every table owns almost the same hot ratio.
  const uint64_t offset = cuid % static_cast<uint64_t>(kCuidsPerTable);
  const uint64_t mixed =
      (offset * 1103515245ULL + 12345ULL) % static_cast<uint64_t>(kCuidsPerTable);
  const uint64_t hot_count = std::max<uint64_t>(
      1, static_cast<uint64_t>(kCuidsPerTable * kHotRatio));
  return mixed < hot_count;
}

std::string GenerateDbUpperBoundKey() {
  std::string key(40, '\0');
  EncodeUint64BE(kDbId + 1, &key[0]);
  return key;
}

std::string GenerateKey(uint64_t table_id, uint64_t cuid, int row_id) {
  std::string key;
  key.resize(40);
  std::memset(&key[0], 0, 40);
  EncodeUint64BE(kDbId, &key[0]);
  EncodeUint64BE(table_id, &key[8]);
  EncodeUint64BE(cuid, &key[16]);
  char row_buf[16];
  snprintf(row_buf, sizeof(row_buf), "%010d", row_id);
  std::memcpy(&key[24], row_buf, 10);
  return key;
}

uint64_t ExtractCUID(const rocksdb::Slice& key) {
  if (key.size() < 24) return 0;
  const unsigned char* p = reinterpret_cast<const unsigned char*>(key.data()) + 16;
  uint64_t c = 0;
  for (int i = 0; i < 8; ++i) {
    c = (c << 8) | p[i];
  }
  return c;
}

struct ThreadStats {
  uint64_t put_ops = 0;
  uint64_t scan_ops = 0;
  uint64_t del_ops = 0;
  uint64_t total_rows_scanned = 0;
  std::vector<double> put_latencies;
  std::vector<double> scan_latencies;
  std::vector<double> del_latencies;

  void AddPut(double ms) {
    put_ops++;
    if (put_latencies.size() < 1000000) put_latencies.push_back(ms);
  }
  void AddScan(double ms, uint64_t rows) {
    scan_ops++;
    total_rows_scanned += rows;
    if (scan_latencies.size() < 1000000) scan_latencies.push_back(ms);
  }
  void AddDelete(double ms) {
    del_ops++;
    if (del_latencies.size() < 1000000) del_latencies.push_back(ms);
  }
};

class PerfRunner {
 public:
  PerfRunner(rocksdb::DB* db, std::atomic<uint64_t>* next_cuid,
             bool partition_enabled)
      : db_(db),
        next_cuid_(next_cuid),
        partition_enabled_(partition_enabled) {}

  void Run(int thread_id, std::atomic<bool>* stop, ThreadStats* stats) {
    std::default_random_engine gen(thread_id + static_cast<int>(time(0)));
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    rocksdb::ReadOptions read_opts;

    while (!stop->load()) {
      // 1. 领取一个全新的 CUID
      const uint64_t logical_idx = next_cuid_->fetch_add(1);
      if (logical_idx >= static_cast<uint64_t>(kNumCuids)) break;
      const uint64_t cuid = LogicalIndexToCUID(logical_idx);
      const uint64_t table_id = TableIdFromCUID(cuid);

      // 2. 状态初始化
      // Spread hot CUID evenly across all table IDs to avoid hot-table skew.
      bool is_hot = IsHotCUIDUniformAcrossTables(cuid);
      int target_scans = is_hot ? kHotScanTarget : kColdScanTarget;
      int puts_done = 0;
      int scans_done = 0;

      // 3. 执行单 CUID 顺序周期：交替进行写入和读取
      while (puts_done < kTargetPutBatches || scans_done < target_scans) {
        if (stop->load()) return;

        bool do_put = false;
        if (puts_done < kTargetPutBatches) {
          if (scans_done < target_scans) {
            // 根据进度比例交替，确保 Put 均匀分布在整个 Scan 周期中
            if ((double)puts_done / kTargetPutBatches <= (double)scans_done / target_scans) {
              do_put = true;
            } else {
              do_put = false;
            }
          } else {
            do_put = true; // 只剩 Put 没做了
          }
        } else {
          do_put = false; // 只剩 Scan 没做了
        }

        if (do_put) {
          DoPut(table_id, cuid, puts_done * kBatchSize, stats);
          puts_done++;
        } else {
          bool full_scan = (dist(gen) < 0.1);
          DoScan(table_id, cuid, puts_done * kBatchSize, full_scan, read_opts,
                 stats);
          scans_done++;
        }
      }

      // 4. 终结清理：最后一次 Full Scan -> 逐一物理删除
      int final_rows = puts_done * kBatchSize;
      int final_scan_count =
          DoScan(table_id, cuid, final_rows, true, read_opts, stats);
      if (final_scan_count != final_rows) {
        std::cerr << "[ERROR] Thread " << thread_id << " - CUID " << cuid 
                  << " (TableID " << table_id << ")"
                  << " final full scan mismatch! Expected: " << final_rows 
                  << ", Actual: " << final_scan_count << std::endl;
      }
      DoDelete(table_id, cuid, final_rows, stats);

      // std::cout << "[Thread " << thread_id << "] Completed CUID " << cuid
      //           << " (Rows: " << final_rows << ", Scans: " << scans_done
      //           << ", Hot: " << is_hot << ")" << std::endl;
    }
  }

 private:
  void DoPut(uint64_t table_id, uint64_t cuid, int start_row,
             ThreadStats* stats) {
    rocksdb::WriteBatch batch;
    for (int i = 0; i < kBatchSize; ++i) {
      batch.Put(GenerateKey(table_id, cuid, start_row + i),
                "value_data_payload_xxxxxxx");
    }
    auto start = std::chrono::high_resolution_clock::now();
    rocksdb::Status s = db_->Write(rocksdb::WriteOptions(), &batch);
    auto end = std::chrono::high_resolution_clock::now();
    stats->AddPut(std::chrono::duration<double, std::milli>(end - start).count());
  }

  int DoScan(uint64_t table_id, uint64_t cuid, int cur_rows, bool full_scan,
             const rocksdb::ReadOptions& ro, ThreadStats* stats) {
    std::string start_key, end_key;
    if (full_scan) {
      start_key = GenerateKey(table_id, cuid, 0);
      if (cuid + 1 < static_cast<uint64_t>(kNumCuids)) {
        const uint64_t next_cuid = cuid + 1;
        const uint64_t next_table_id = TableIdFromCUID(next_cuid);
        end_key = GenerateKey(next_table_id, next_cuid, 0);
      } else {
        end_key = GenerateDbUpperBoundKey();
      }
    } else {
      int offset = std::max(0, cur_rows - 500);
      start_key = GenerateKey(table_id, cuid, offset);
      end_key = GenerateKey(table_id, cuid, cur_rows);
    }

    rocksdb::Slice upper_bound(end_key);
    rocksdb::Slice lower_bound(start_key);
    rocksdb::ReadOptions ro_copy = ro;
    ro_copy.iterate_upper_bound = &upper_bound;
    ro_copy.iterate_lower_bound = &lower_bound;
    ro_copy.delta_full_scan = full_scan;
    if (partition_enabled_) {
      ro_copy.read_partition_id = PartitionIdFromTableId(table_id);
    }

    auto t_start = std::chrono::high_resolution_clock::now();
    rocksdb::Iterator* it = db_->NewIterator(ro_copy);
    it->Seek(start_key);

    int count = 0;
    while (it->Valid()) {
      if (ExtractCUID(it->key()) != cuid) break;
      count++;
      it->Next();
    }
    delete it;
    auto t_end = std::chrono::high_resolution_clock::now();
    stats->AddScan(std::chrono::duration<double, std::milli>(t_end - t_start).count(), count);

    return count;
  }

  void DoDelete(uint64_t table_id, uint64_t cuid, int total_rows,
                ThreadStats* stats) {
    rocksdb::WriteOptions wo;
    for (int i = 0; i < total_rows; ++i) {
      auto start = std::chrono::high_resolution_clock::now();
      db_->Delete(wo, GenerateKey(table_id, cuid, i));
      auto end = std::chrono::high_resolution_clock::now();
      stats->AddDelete(std::chrono::duration<double, std::milli>(end - start).count());
    }
  }

  rocksdb::DB* db_;
  std::atomic<uint64_t>* next_cuid_;
  bool partition_enabled_;
};

struct AggregateStats {
  uint64_t total_puts = 0;
  uint64_t total_scans = 0;
  uint64_t total_dels = 0;
  uint64_t throughput_rows_per_sec = 0;
  double put_avg_ms = 0.0;
  double put_p99_ms = 0.0;
  double scan_avg_ms = 0.0;
  double scan_p99_ms = 0.0;
  double del_avg_ms = 0.0;
  double del_p99_ms = 0.0;
};

AggregateStats ReportStats(const std::string& label,
                          const std::vector<ThreadStats>& all_stats,
                          int duration_sec) {
  AggregateStats stats;
  uint64_t total_puts = 0, total_scans = 0, total_dels = 0, total_rows = 0;
  std::vector<double> all_put_lat, all_scan_lat, all_del_lat;
  
  for (const auto& s : all_stats) {
    total_puts += s.put_ops;
    total_scans += s.scan_ops;
    total_dels += s.del_ops;
    total_rows += s.total_rows_scanned;
    all_put_lat.insert(all_put_lat.end(), s.put_latencies.begin(), s.put_latencies.end());
    all_scan_lat.insert(all_scan_lat.end(), s.scan_latencies.begin(), s.scan_latencies.end());
    all_del_lat.insert(all_del_lat.end(), s.del_latencies.begin(), s.del_latencies.end());
  }

  auto get_avg = [](std::vector<double>& latencies) {
    if (latencies.empty()) return 0.0;
    double sum = 0;
    for (double d : latencies) sum += d;
    return sum / latencies.size();
  };

  auto get_p99 = [](std::vector<double>& latencies) {
    if (latencies.empty()) return 0.0;
    std::sort(latencies.begin(), latencies.end());
    return latencies[static_cast<size_t>(latencies.size() * 0.99)];
  };

  stats.total_puts = total_puts;
  stats.total_scans = total_scans;
  stats.total_dels = total_dels;
  stats.put_avg_ms = get_avg(all_put_lat);
  stats.put_p99_ms = get_p99(all_put_lat);
  stats.scan_avg_ms = get_avg(all_scan_lat);
  stats.scan_p99_ms = get_p99(all_scan_lat);
  stats.del_avg_ms = get_avg(all_del_lat);
  stats.del_p99_ms = get_p99(all_del_lat);
  stats.throughput_rows_per_sec =
      (duration_sec > 0 ? total_rows / static_cast<uint64_t>(duration_sec) : 0);

  std::cout << "\n--- " << label << " Results ---" << std::endl;
  std::cout << "Put (Batch 128) Ops: " << total_puts << ", Avg Lat: "
            << std::fixed << std::setprecision(3) << stats.put_avg_ms
            << " ms, P99 Lat: " << stats.put_p99_ms << " ms" << std::endl;
  std::cout << "Scan Ops: " << total_scans << ", Avg Lat: "
            << stats.scan_avg_ms << " ms, P99 Lat: " << stats.scan_p99_ms
            << " ms" << std::endl;
  std::cout << "Delete Ops: " << total_dels << ", Avg Lat: "
            << stats.del_avg_ms << " ms, P99 Lat: " << stats.del_p99_ms
            << " ms" << std::endl;
  std::cout << "Throughput: " << stats.throughput_rows_per_sec << " rows/s"
            << std::endl;

  return stats;
}

double PercentChange(double before, double after) {
  if (before == 0.0) {
    return 0.0;
  }
  return (after - before) * 100.0 / before;
}

void ReportModeComparison(const AggregateStats& no_delta_no_partition,
           const AggregateStats& delta_with_partition) {
  std::cout << "\n=== Delta+Partition vs Native ===" << std::endl;
  std::cout << "Scan Avg Lat Change: "
       << PercentChange(no_delta_no_partition.scan_avg_ms,
         delta_with_partition.scan_avg_ms)
            << "%" << std::endl;
  std::cout << "Scan P99 Lat Change: "
       << PercentChange(no_delta_no_partition.scan_p99_ms,
         delta_with_partition.scan_p99_ms)
            << "%" << std::endl;
  std::cout << "Put Avg Lat Change: "
       << PercentChange(no_delta_no_partition.put_avg_ms,
         delta_with_partition.put_avg_ms)
            << "%" << std::endl;
  std::cout << "Put P99 Lat Change: "
       << PercentChange(no_delta_no_partition.put_p99_ms,
         delta_with_partition.put_p99_ms)
            << "%" << std::endl;
  std::cout << "Throughput Change: "
       << PercentChange(
         static_cast<double>(no_delta_no_partition.throughput_rows_per_sec),
         static_cast<double>(delta_with_partition.throughput_rows_per_sec))
            << "%" << std::endl;
}

int ResolveDurationSec() {
  const char* env_duration = std::getenv("ROCKSDB_SCAN_DURATION_SEC");
  if (env_duration == nullptr || env_duration[0] == '\0') {
    return kDefaultTestDurationSec;
  }
  const int parsed = std::atoi(env_duration);
  return parsed > 0 ? parsed : kDefaultTestDurationSec;
}

int main() {
  static_assert(kNumTableIds % kActiveTableWindow == 0,
                "kNumTableIds must be divisible by kActiveTableWindow");

  CleanupDBPath(kNativeDBPath);
  CleanupDBPath(kDeltaDBPath);

  const int test_duration_sec = ResolveDurationSec();

  std::atomic<uint64_t> next_cuid_counter{0};

  auto run_benchmark = [&](const std::string& path, bool delta_enabled,
                           bool partition_enabled, const std::string& label,
                           AggregateStats* out_stats) -> bool {
    rocksdb::Options options;
    options.create_if_missing = true;
    
    if (delta_enabled) {
      options.enable_delta = true;
      options.delta_options.enable_partition = partition_enabled;

      // --- Example 1: Programmatic Configuration of DeltaOptions ---
      // These can be set directly on the options object before opening the DB.
      options.delta_options.hotspot_scan_threshold = 200;
      options.delta_options.hotspot_scan_window_sec = 300;
      options.delta_options.delta_merge_threshold = 3;
      options.delta_options.sac_delta_count_threshold = 5;
      options.delta_options.sharding_count = 128; // Power of 2 recommended
      options.delta_options.hot_data_buffer_threshold_bytes = 64 * 1024 * 1024;
      options.delta_options.hot_data_buffer_shards = 128;
      options.delta_options.compaction_l0_trigger_count = 48;
      options.delta_options.compaction_l0_partition_trigger_count = 8;
      options.delta_options.compaction_l0_trigger_age_sec = 3600;
      options.delta_options.compaction_l0_files_to_pick = 6;
      // -------------------------------------------------------------
      options.write_buffer_size = 256 * 1024 * 1024;
      options.level0_slowdown_writes_trigger = 200; // l0 file count thres
      options.level0_stop_writes_trigger = 400; // l0 file count thres
      options.level0_file_num_compaction_trigger = 48; // l0 file count thres
      options.soft_pending_compaction_bytes_limit = 0; // 0 表示无限制
      options.hard_pending_compaction_bytes_limit = 0; // 0 表示无限制
      options.max_background_jobs = 16; // 与写入线程相同？
      options.num_levels = 1;
      options.level_compaction_dynamic_level_bytes = false;
    } else {
      options.enable_delta = false;
      options.max_background_jobs = 16;
    }

    rocksdb::DestroyDB(path, options);
    
    rocksdb::DB* db;
    rocksdb::Status s = rocksdb::DB::Open(options, path, &db);
    if (!s.ok()) {
      std::cerr << "Open " << label << " failed: " << s.ToString() << std::endl;
      return false;
    }

    std::cout << ">>> Running " << label << " (Duration: " << test_duration_sec << "s) <<<" << std::endl;
    next_cuid_counter.store(0);

    std::atomic<bool> stop{false};
    std::vector<ThreadStats> all_thread_stats(kNumThreads);
    std::vector<std::thread> workers;
    PerfRunner runner(db, &next_cuid_counter, delta_enabled && partition_enabled);

    for (int i = 0; i < kNumThreads; i++) {
      workers.emplace_back(&PerfRunner::Run, &runner, i, &stop, &all_thread_stats[i]);
    }

    std::this_thread::sleep_for(std::chrono::seconds(test_duration_sec));
    stop.store(true);
    for (auto& t : workers) t.join();

    *out_stats = ReportStats(label, all_thread_stats, test_duration_sec);
    delete db;
    return true;
  };

  AggregateStats no_delta_no_partition;
  AggregateStats delta_with_partition;

  // if (!run_benchmark(kNativeDBPath, false, false,
  //                    "NATIVE MODE (NO DELTA/PARTITION)",
  //                    &no_delta_no_partition)) {
  //   return 1;
  // }
  if (!run_benchmark(kDeltaDBPath, true, true,
                     "DELTA MODE (PARTITION ON)",
                     &delta_with_partition)) {
    return 1;
  }

  // ReportModeComparison(no_delta_no_partition, delta_with_partition);

  return 0;
}