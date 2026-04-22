/**
 * rocksdb_perf_test.cc
 *
 * RocksDB-delta 高性能并发测试工具
 *
 * 负载模型：
 * 1. 整体测试时长被均分为 4 个连续 phase，每个 phase 固定一个 table_id。
 * 2. 不同 table_id 之间严格串行，不会并行产生写入/扫描。
 * 3. 单个 phase 内部仍保持多线程、多 CUID 的并发增删改查生命周期。
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
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
#include "rocksdb/statistics.h"

// ==========================================
// 配置常量
// ==========================================
const std::string kNativeDBPath = "/home/jx/Rocksdb-delta/db_perf_test/db_perf_native";
const std::string kDeltaDBPath = "/home/jx/Rocksdb-delta/db_perf_test/db_perf_delta";
const std::string kDeltaPartitionDBPath = "/home/jx/Rocksdb-delta/db_perf_test/db_perf_delta_partition";
const int kNumThreads = 32;
const int kTestDurationSec = 3600;       // s
const int kNumCuids = 1000000;           // 100W CUID 总库
const uint64_t kNumTableIds = 4;
const uint64_t kDbId = 1;
const int kBatchSize = 512;             // 每次 Put 512 行
const int kTargetPutBatches = 240;      // 每个 CUID 固定写入 240 个 batch (目标约 12W 行)
const int kMinPutBatchesBeforeScan = 16;  // 先完成一小段写入预热，再开始扫描
const double kHotRatio = 0.15;          // 15% 的热点
const int kHotScanTarget = 24;         // 热点 CUID 的部分扫描次数
const int kColdScanTarget = 4;         // 普通 CUID 的部分扫描次数
const size_t kDeleteWindowSize = 24;   // 延迟删除窗口，保证单个 phase 内能真实触发 delete

// ==========================================
// 辅助工具与状态管理
// ==========================================

bool CleanupDBPath(const std::string& path) {
  std::error_code ec;
  if (path != kNativeDBPath && path != kDeltaDBPath &&
      path != kDeltaPartitionDBPath) {
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

int32_t PartitionIdFromTableId(uint64_t table_id) {
  return static_cast<int32_t>(table_id % 16);
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

void MergeThreadStats(ThreadStats* dst, const ThreadStats& src) {
  if (dst == nullptr) {
    return;
  }

  constexpr size_t kMaxLatencySamples = 1000000;
  auto merge_latencies = [](std::vector<double>* target,
                            const std::vector<double>& source) {
    constexpr size_t kMaxLatencySamplesLocal = 1000000;
    if (target == nullptr || target->size() >= kMaxLatencySamplesLocal) {
      return;
    }
    const size_t remaining = kMaxLatencySamplesLocal - target->size();
    const size_t to_copy = std::min(remaining, source.size());
    target->insert(target->end(), source.begin(), source.begin() + to_copy);
  };

  dst->put_ops += src.put_ops;
  dst->scan_ops += src.scan_ops;
  dst->del_ops += src.del_ops;
  dst->total_rows_scanned += src.total_rows_scanned;
  if (dst->put_latencies.size() < kMaxLatencySamples) {
    merge_latencies(&dst->put_latencies, src.put_latencies);
  }
  if (dst->scan_latencies.size() < kMaxLatencySamples) {
    merge_latencies(&dst->scan_latencies, src.scan_latencies);
  }
  if (dst->del_latencies.size() < kMaxLatencySamples) {
    merge_latencies(&dst->del_latencies, src.del_latencies);
  }
}

class PerfRunner {
 public:
  PerfRunner(rocksdb::DB* db, std::atomic<uint64_t>* next_cuid,
             uint64_t table_id,
             bool partition_enabled)
      : db_(db),
        next_cuid_(next_cuid),
        table_id_(table_id),
        partition_enabled_(partition_enabled) {}

    struct CuidState {
      uint64_t cuid;
      bool is_hot;
      int target_scans;
      int puts_done = 0;
      int scans_done = 0;
      std::chrono::steady_clock::time_point last_scan_time = std::chrono::steady_clock::now();
    };

  void Run(int thread_id, std::atomic<bool>* stop, ThreadStats* stats) {
    std::default_random_engine gen(thread_id + static_cast<int>(time(0)));
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    rocksdb::ReadOptions read_opts;
    
    std::vector<CuidState> active_cuids;
    std::deque<std::pair<uint64_t, int>> pending_deletes;
    
    // 经验值：控制活跃/待删除窗口，既保持足够并发，也避免单线程积压过多生命周期。
    const size_t kMaxActiveCuids = 50;

    auto add_new_cuid = [&]() {
      uint64_t cuid = next_cuid_->fetch_add(1);
      if (cuid > kNumCuids) return false;
      CuidState state;
      state.cuid = cuid;
      state.is_hot = (((cuid * 1103515245 + 12345) / 65536) % 100 < (kHotRatio * 100));
      state.target_scans = state.is_hot ? kHotScanTarget : kColdScanTarget;
      active_cuids.push_back(state);
      return true;
    };

    // 1. 初始化每个线程的活跃工作池
    for (size_t i = 0; i < kMaxActiveCuids; ++i) {
      if (!add_new_cuid()) break;
    }

    while (!stop->load() && !active_cuids.empty()) {
      // 2. 随机挑选一个没有走完生命周期的活跃 CUID
      std::uniform_int_distribution<size_t> idx_dist(0, active_cuids.size() - 1);
      size_t idx = idx_dist(gen);
      auto& state = active_cuids[idx];

      bool do_put = false;
      if (state.puts_done < kTargetPutBatches) {
        if (state.puts_done < kMinPutBatchesBeforeScan) {
          do_put = true;
        } else if (state.scans_done < state.target_scans) {
          // 根据进度比例交替，但整体上显著偏向写入，避免 Scan 过于频繁。
          if ((double)state.puts_done / kTargetPutBatches <=
              (double)state.scans_done / state.target_scans) {
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


      // 3. 执行动作
      if (do_put) {
        if (DoPut(state.cuid, state.puts_done * kBatchSize, stats)) {
          state.puts_done++;
        }
      } else {
        // 生命周期内的常规扫描一律使用 partial scan；删除前再做一次 full scan 校验。
        DoScan(state.cuid, state.puts_done * kBatchSize, false, read_opts,
               stats, gen);
        state.scans_done++;
        // state.last_scan_time = now;
      }

      // 模拟业务节奏
      std::this_thread::sleep_for(std::chrono::milliseconds(5));

      // 4. 判断该 CUID 是否终于走完了完整的轮回
      if (state.puts_done >= kTargetPutBatches && state.scans_done >= state.target_scans) {
        int final_rows = state.puts_done * kBatchSize;
        int final_scan_count =
          DoScan(state.cuid, final_rows, true, read_opts, stats, gen);
        if (final_scan_count != final_rows) {
          std::cerr << "[ERROR] Thread " << thread_id << " - CUID " << state.cuid 
                    << " final full scan mismatch! Expected: " << final_rows 
                    << ", Actual: " << final_scan_count << std::endl;
        }
        
        // 移入延迟删除队列（不马上删，让数据有命沉淀到底层）
        pending_deletes.push_back({state.cuid, final_rows});
        active_cuids.erase(active_cuids.begin() + idx); 

        // 补充一个新的血液到池子里
        add_new_cuid();

        // 推进删除滑动时间窗
        if (pending_deletes.size() > kDeleteWindowSize) {
          auto oldest = pending_deletes.front();
          pending_deletes.pop_front();
          DoDelete(oldest.first, oldest.second, stats);
        }
      }
    }

    // 线程退出前，如果时间已经到了 (stop->load() 为 true)
    // 那么直接放弃收尾清理 pending_deletes，以避免测试停滞无法结束。
    // 在真实应用中系统退出也不需要把全部数据删掉。
    if (!stop->load()) {
      while (!pending_deletes.empty()) {
        auto oldest = pending_deletes.front();
        pending_deletes.pop_front();
        DoDelete(oldest.first, oldest.second, stats);
      }
    }
  }

 private:
  bool DoPut(uint64_t cuid, int start_row, ThreadStats* stats) {
    rocksdb::WriteBatch batch;
    for (int i = 0; i < kBatchSize; ++i) {
      batch.Put(GenerateKey(table_id_, cuid, start_row + i),
                "value_data_payload_xxxxxxxxxxxxxxxxxxxx");
    }
    auto start = std::chrono::high_resolution_clock::now();
    rocksdb::Status s = db_->Write(rocksdb::WriteOptions(), &batch);
    auto end = std::chrono::high_resolution_clock::now();
    if (!s.ok()) {
      return false;
    }
    stats->AddPut(std::chrono::duration<double, std::milli>(end - start).count());
    return true;
  }

  int DoScan(uint64_t cuid, int cur_rows, bool full_scan, const rocksdb::ReadOptions& ro, ThreadStats* stats, std::default_random_engine& gen) {
    std::string start_key, end_key;
    if (full_scan) {
      start_key = GenerateKey(table_id_, cuid, 0);
      end_key = GenerateKey(table_id_, cuid, cur_rows);
    } else {
      // 随机扫描 10%-75% 的数据范围
      if (cur_rows < 10) {
          start_key = GenerateKey(table_id_, cuid, 0);
          end_key = GenerateKey(table_id_, cuid, cur_rows);
      } else {
          int min_len = static_cast<int>(cur_rows * 0.1);
          int max_len = static_cast<int>(cur_rows * 0.75);
          std::uniform_int_distribution<int> len_dist(min_len, std::max(min_len + 1, max_len));
          int scan_len = len_dist(gen);
          
          std::uniform_int_distribution<int> start_dist(0, std::max(0, cur_rows - scan_len));
          int start_row = start_dist(gen);
          start_key = GenerateKey(table_id_, cuid, start_row);
          end_key = GenerateKey(table_id_, cuid, start_row + scan_len);
      }
    }

    rocksdb::Slice upper_bound(end_key);
    rocksdb::ReadOptions ro_copy = ro;
    ro_copy.iterate_upper_bound = &upper_bound;
    ro_copy.delta_full_scan = full_scan;
    if (partition_enabled_) {
      ro_copy.read_partition_id = PartitionIdFromTableId(table_id_);
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

  void DoDelete(uint64_t cuid, int total_rows, ThreadStats* stats) {
    rocksdb::WriteOptions wo;
    for (int i = 0; i < total_rows; ++i) {
      auto start = std::chrono::high_resolution_clock::now();
      db_->Delete(wo, GenerateKey(table_id_, cuid, i));
      auto end = std::chrono::high_resolution_clock::now();
      stats->AddDelete(std::chrono::duration<double, std::milli>(end - start).count());
    }
  }

  rocksdb::DB* db_;
  std::atomic<uint64_t>* next_cuid_;
  uint64_t table_id_;
  bool partition_enabled_;
};

void ReportStats(const std::string& label, const std::vector<ThreadStats>& all_stats) {
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

  std::cout << "\n--- " << label << " Results ---" << std::endl;
  std::cout << "Put (Batch " << kBatchSize << ") Ops: " << total_puts
            << ", Avg Lat: " << std::fixed << std::setprecision(3)
            << get_avg(all_put_lat) << " ms, P99 Lat: " << get_p99(all_put_lat)
            << " ms" << std::endl;
  std::cout << "Scan Ops: " << total_scans << ", Avg Lat: " << get_avg(all_scan_lat) << " ms, P99 Lat: " << get_p99(all_scan_lat) << " ms" << std::endl;
  std::cout << "Delete Ops: " << total_dels << ", Avg Lat: " << get_avg(all_del_lat) << " ms, P99 Lat: " << get_p99(all_del_lat) << " ms" << std::endl;
  std::cout << "Throughput: " << (kTestDurationSec > 0 ? total_rows / kTestDurationSec : 0) << " rows/s" << std::endl;
}

int main() {
  CleanupDBPath(kNativeDBPath);
  CleanupDBPath(kDeltaDBPath);
  CleanupDBPath(kDeltaPartitionDBPath);

  std::atomic<uint64_t> next_cuid_counter{1};

  auto run_benchmark = [&](const std::string& path, bool delta_enabled,
                           bool partition_enabled, const std::string& label) {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.statistics = rocksdb::CreateDBStatistics();
    
    if (delta_enabled) {
      options.enable_delta = true;
      options.delta_options.enable_partition = partition_enabled;

      // --- Example 1: Programmatic Configuration of DeltaOptions ---
      // These can be set directly on the options object before opening the DB.
      options.delta_options.hotspot_scan_threshold = 16;
      options.delta_options.hotspot_scan_window_sec = 300;
      options.delta_options.delta_merge_threshold = 3;
      options.delta_options.sac_delta_count_threshold = 5;
      options.delta_options.sharding_count = 64; // Power of 2 recommended
      options.delta_options.max_delta_threads = 4;
      options.delta_options.hot_data_buffer_threshold_bytes = 64 * 1024 * 1024;
      options.delta_options.hot_data_buffer_shards = 128;
      options.delta_options.compaction_l0_trigger_count = 30;
      options.delta_options.compaction_l0_partition_trigger_count = 30;
      options.delta_options.compaction_l0_trigger_age_sec = 3600;
      options.delta_options.compaction_l0_files_to_pick = 10;
      // -------------------------------------------------------------
      options.level0_slowdown_writes_trigger = 200; // l0 file count thres
      options.level0_stop_writes_trigger = 400; // l0 file count thres
      options.level0_file_num_compaction_trigger = 100; // l0 file count thres
      options.max_subcompactions = 4; // subcompaction 线程数
      options.soft_pending_compaction_bytes_limit = 0; // 0 表示无限制
      options.hard_pending_compaction_bytes_limit = 0; // 0 表示无限制
      options.max_background_jobs = 32; // 与写入线程相同？
      options.num_levels = 1;
      options.level_compaction_dynamic_level_bytes = false;
      if(partition_enabled){
        options.write_buffer_size = 256 * 1024 * 1024;
      }
    } else {
      options.enable_delta = false;
      options.max_background_jobs = 16;
    }

    rocksdb::DestroyDB(path, options);
    
    rocksdb::DB* db;
    rocksdb::Status s = rocksdb::DB::Open(options, path, &db);
    if (!s.ok()) {
      std::cerr << "Open " << label << " failed: " << s.ToString() << std::endl;
      return;
    }

    std::cout << ">>> Running " << label << " (Duration: " << kTestDurationSec << "s) <<<" << std::endl;
    next_cuid_counter.store(1);

    std::vector<ThreadStats> all_thread_stats(kNumThreads);
    const int64_t phase_duration_sec =
        kTestDurationSec / static_cast<int64_t>(kNumTableIds);
    const int64_t phase_remainder_sec =
        kTestDurationSec % static_cast<int64_t>(kNumTableIds);

    for (uint64_t phase_idx = 0; phase_idx < kNumTableIds; ++phase_idx) {
      const uint64_t table_id = phase_idx + 1;
      int64_t current_phase_sec = phase_duration_sec;
      if (phase_idx + 1 == kNumTableIds) {
        current_phase_sec += phase_remainder_sec;
      }
      if (current_phase_sec <= 0) {
        break;
      }

      std::cout << "  -> Phase " << (phase_idx + 1) << "/" << kNumTableIds
                << ": TableID " << table_id << ", Duration: "
                << current_phase_sec << "s" << std::endl;

      std::atomic<bool> stop{false};
      std::vector<ThreadStats> phase_thread_stats(kNumThreads);
      std::vector<std::thread> workers;
      PerfRunner runner(db, &next_cuid_counter, table_id,
                        delta_enabled && partition_enabled);

      for (int i = 0; i < kNumThreads; i++) {
        workers.emplace_back(&PerfRunner::Run, &runner, i, &stop,
                             &phase_thread_stats[i]);
      }

      std::this_thread::sleep_for(std::chrono::seconds(current_phase_sec));
      stop.store(true);
      for (auto& t : workers) t.join();

      for (int i = 0; i < kNumThreads; ++i) {
        MergeThreadStats(&all_thread_stats[i], phase_thread_stats[i]);
      }
    }

    ReportStats(label, all_thread_stats);
    
    // Output comprehensive DB statistics
    std::string stats;
    if (db->GetProperty(rocksdb::DB::Properties::kStats, &stats)) {
      std::cout << "\n=== DB Statistics for " << label << " ===\n";
      std::cout << stats << std::endl;
      std::cout << "=====================================\n";
    }

    // Output Level Stats clearly
    std::string level_stats;
    if (db->GetProperty(rocksdb::DB::Properties::kLevelStats, &level_stats)) {
      std::cout << "\n=== Level File Count Statistics (" << label << ") ===\n";
      std::cout << level_stats << std::endl;
      std::cout << "=====================================\n";
    }

    delete db;
  };
  run_benchmark(kNativeDBPath, false, false, "NATIVE MODE");
  // run_benchmark(kDeltaDBPath, true, false, "DELTA MODE");
  // run_benchmark(kDeltaPartitionDBPath, true, true, "Partition MODE");
  return 0;
}
