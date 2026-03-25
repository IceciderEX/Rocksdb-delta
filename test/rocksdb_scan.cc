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
#include "db/db_impl/db_impl.h"
#include "delta/hotspot_manager.h"
#include "util/extract_cuid.h"
#include "delta/hot_index_table.h"

// ==========================================
// 配置常量
// ==========================================
const std::string kNativeDBPath = "/home/wam/Rocksdb-delta/db_perf_test/db_perf_native";
const std::string kDeltaDBPath = "/home/wam/Rocksdb-delta/db_perf_test/db_perf_delta";
const int kNumThreads = 16;
const int kTestDurationSec = 600;       // s
const int kNumCuids = 100000;           // 10W CUID 总库
const int kBatchSize = 128;             // 每次 Put 128 行
const int kTargetPutBatches = 200;      // 每个 CUID 固定写入 200 个 batch (约 25,600 行)
const double kHotRatio = 0.15;          // 15% 的热点
const int kHotScanTarget = 1000;        // 热点访问目标
const int kColdScanTarget = 150;        // 普通访问目标

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

std::string GenerateKey(uint64_t cuid, int row_id) {
  std::string key;
  key.resize(40);
  std::memset(&key[0], 0, 40);
  unsigned char* p = reinterpret_cast<unsigned char*>(&key[0]) + 16;
  for (int i = 0; i < 8; ++i) {
    p[i] = (cuid >> (56 - 8 * i)) & 0xFF; // Big Endian
  }
  char row_buf[16];
  snprintf(row_buf, sizeof(row_buf), "%010d", row_id);
  std::memcpy(&key[24], row_buf, 10);
  return key;
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
  PerfRunner(rocksdb::DB* db, std::atomic<uint64_t>* next_cuid) 
      : db_(db), next_cuid_(next_cuid) {}

  struct CuidState {
    uint64_t cuid;
    bool is_hot;
    int target_scans;
    int puts_done = 0;
    int scans_done = 0;
  };

  void Run(int thread_id, std::atomic<bool>* stop, ThreadStats* stats) {
    std::default_random_engine gen(thread_id + static_cast<int>(time(0)));
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    rocksdb::ReadOptions read_opts;
    
    std::vector<CuidState> active_cuids;
    std::deque<std::pair<uint64_t, int>> pending_deletes;
    
    // === 调整生命周期参数 ===
    // 之前设置了 50+50 导致240秒内根本填不满滑动窗口，没触发删除
    // 现在调整为：并发25个活跃 + 10个等待删除 = 35个/线程
    // 16线程全局共有约 560 个 CUID，超过 1GB 数据，足以压入 L1/L2
    const size_t kMaxActiveCuids = 25;   
    const size_t kDeleteWindowSize = 10; 

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
        if (state.scans_done < state.target_scans) {
          // 根据进度比例交替，确保 Put 均匀分布在整个 Scan 周期中
          if ((double)state.puts_done / kTargetPutBatches <= (double)state.scans_done / state.target_scans) {
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

      // 3. 对随机到的 CUID 切片执行一步动作
      if (do_put) {
        DoPut(state.cuid, state.puts_done * kBatchSize, stats);
        state.puts_done++;
      } else {
        bool full_scan = (dist(gen) < 0.1);
        DoScan(state.cuid, state.puts_done * kBatchSize, full_scan, read_opts, stats);
        state.scans_done++;
      }

      // 4. 判断该 CUID 是否终于走完了完整的轮回
      if (state.puts_done >= kTargetPutBatches && state.scans_done >= state.target_scans) {
        int final_rows = state.puts_done * kBatchSize;
        int final_scan_count = DoScan(state.cuid, final_rows, true, read_opts, stats);
        if (final_scan_count != final_rows) {
          std::cerr << "[ERROR] Thread " << thread_id << " - CUID " << state.cuid 
                    << " final full scan mismatch! Expected: " << final_rows 
                    << ", Actual: " << final_scan_count << std::endl;

          // Diagnostic: dump HotIndexEntry state
          auto hotspot_mgr = dynamic_cast<rocksdb::DBImpl*>(db_)->GetHotspotManager();
          if (hotspot_mgr) {
            rocksdb::HotIndexEntry diag_entry;
            if (hotspot_mgr->GetHotIndexEntry(state.cuid, &diag_entry)) {
              std::cerr << "[DIAG] CUID " << state.cuid << " snapshot_segments="
                        << diag_entry.snapshot_segments.size()
                        << " deltas=" << diag_entry.deltas.size() << std::endl;
              for (size_t si = 0; si < diag_entry.snapshot_segments.size(); si++) {
                const auto& seg = diag_entry.snapshot_segments[si];
                std::cerr << "[DIAG]   snap[" << si
                          << "] file=" << (int64_t)seg.file_number
                          << " first_key=" << rocksdb::FormatKeyDisplay(seg.first_key)
                          << " last_key=" << rocksdb::FormatKeyDisplay(seg.last_key)
                          << std::endl;
              }
              for (size_t di = 0; di < diag_entry.deltas.size(); di++) {
                const auto& seg = diag_entry.deltas[di];
                std::cerr << "[DIAG]   delta[" << di
                          << "] file=" << (int64_t)seg.file_number
                          << " first_key=" << rocksdb::FormatKeyDisplay(seg.first_key)
                          << " last_key=" << rocksdb::FormatKeyDisplay(seg.last_key)
                          << std::endl;
              }
            } else {
              std::cerr << "[DIAG] CUID " << state.cuid << " has NO HotIndexEntry!" << std::endl;
            }

            DoScan_Debug(state.cuid, final_rows, true, read_opts, stats);
          }
        }
        
        // 移入延迟删除队列（不马上删，让数据有命沉淀到底层）
        pending_deletes.push_back({state.cuid, final_rows});
        active_cuids.erase(active_cuids.begin() + idx); 

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
  void DoPut(uint64_t cuid, int start_row, ThreadStats* stats) {
    rocksdb::WriteBatch batch;
    for (int i = 0; i < kBatchSize; ++i) {
      batch.Put(GenerateKey(cuid, start_row + i), "value_data_payload_xxxxxxxxxxxxxxxxxxxx");
    }
    auto start = std::chrono::high_resolution_clock::now();
    rocksdb::Status s = db_->Write(rocksdb::WriteOptions(), &batch);
    auto end = std::chrono::high_resolution_clock::now();
    stats->AddPut(std::chrono::duration<double, std::milli>(end - start).count());
  }

  int DoScan(uint64_t cuid, int cur_rows, bool full_scan, const rocksdb::ReadOptions& ro, ThreadStats* stats) {
    std::string start_key, end_key;
    if (full_scan) {
      start_key = GenerateKey(cuid, 0);
      end_key = GenerateKey(cuid + 1, 0);
    } else {
      int offset = std::max(0, cur_rows - 500);
      start_key = GenerateKey(cuid, offset);
      end_key = GenerateKey(cuid, cur_rows);
    }

    rocksdb::Slice upper_bound(end_key);
    rocksdb::ReadOptions ro_copy = ro;
    ro_copy.iterate_upper_bound = &upper_bound;
    ro_copy.delta_full_scan = full_scan;

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

  int DoScan_Debug(uint64_t cuid, int cur_rows, bool full_scan, const rocksdb::ReadOptions& ro, ThreadStats* stats) {
    std::string start_key, end_key;
    if (full_scan) {
      start_key = GenerateKey(cuid, 0);
      end_key = GenerateKey(cuid + 1, 0);
    } else {
      int offset = std::max(0, cur_rows - 500);
      start_key = GenerateKey(cuid, offset);
      end_key = GenerateKey(cuid, cur_rows);
    }

    rocksdb::Slice upper_bound(end_key);
    rocksdb::ReadOptions ro_copy = ro;
    ro_copy.iterate_upper_bound = &upper_bound;
    ro_copy.delta_full_scan = full_scan;

    auto t_start = std::chrono::high_resolution_clock::now();
    rocksdb::Iterator* it = db_->NewIterator(ro_copy);
    it->Seek(start_key);

    int count = 0;
    while (it->Valid()) {
      if (ExtractCUID(it->key()) != cuid) break;
      if (count % 100 == 0) {
        fprintf(stderr, "Scanned key: %s\n", rocksdb::FormatKeyDisplay(it->key()).c_str());
      }
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
      db_->Delete(wo, GenerateKey(cuid, i));
      auto end = std::chrono::high_resolution_clock::now();
      stats->AddDelete(std::chrono::duration<double, std::milli>(end - start).count());
    }
  }

  rocksdb::DB* db_;
  std::atomic<uint64_t>* next_cuid_;
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
  std::cout << "Put (Batch 128) Ops: " << total_puts << ", Avg Lat: " << std::fixed << std::setprecision(3) << get_avg(all_put_lat) << " ms, P99 Lat: " << get_p99(all_put_lat) << " ms" << std::endl;
  std::cout << "Scan Ops: " << total_scans << ", Avg Lat: " << get_avg(all_scan_lat) << " ms, P99 Lat: " << get_p99(all_scan_lat) << " ms" << std::endl;
  std::cout << "Delete Ops: " << total_dels << ", Avg Lat: " << get_avg(all_del_lat) << " ms, P99 Lat: " << get_p99(all_del_lat) << " ms" << std::endl;
  std::cout << "Throughput: " << (kTestDurationSec > 0 ? total_rows / kTestDurationSec : 0) << " rows/s" << std::endl;
}

int main() {
  CleanupDBPath(kNativeDBPath);
  CleanupDBPath(kDeltaDBPath);

  std::atomic<uint64_t> next_cuid_counter{1};

  auto run_benchmark = [&](const std::string& path, bool delta_enabled, const std::string& label) {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.statistics = rocksdb::CreateDBStatistics();
    
    if (delta_enabled) {
      options.enable_delta = true;

      // --- Example 1: Programmatic Configuration of DeltaOptions ---
      // These can be set directly on the options object before opening the DB.
      options.delta_options.hotspot_scan_threshold = 200;
      options.delta_options.hotspot_scan_window_sec = 300;
      options.delta_options.delta_merge_threshold = 3;
      options.delta_options.sac_delta_count_threshold = 5;
      options.delta_options.sharding_count = 64; // Power of 2 recommended
      options.delta_options.hot_data_buffer_threshold_bytes = 64 * 1024 * 1024;
      options.delta_options.hot_data_buffer_shards = 128;
      options.delta_options.compaction_l0_trigger_count = 50;
      options.delta_options.compaction_l0_trigger_age_sec = 3600;
      options.delta_options.compaction_l0_files_to_pick = 10;
      // -------------------------------------------------------------
      options.level0_slowdown_writes_trigger = 1000; // l0 file count thres
      options.level0_stop_writes_trigger = 2000; // l0 file count thres
      options.level0_file_num_compaction_trigger = 100; // l0 file count thres
      options.soft_pending_compaction_bytes_limit = 0; // 0 表示无限制
      options.hard_pending_compaction_bytes_limit = 0; // 0 表示无限制
      options.num_levels = 1;
      options.level_compaction_dynamic_level_bytes = false;
    } else {
      options.enable_delta = false;
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

    std::atomic<bool> stop{false};
    std::vector<ThreadStats> all_thread_stats(kNumThreads);
    std::vector<std::thread> workers;
    PerfRunner runner(db, &next_cuid_counter);

    for (int i = 0; i < kNumThreads; i++) {
      workers.emplace_back(&PerfRunner::Run, &runner, i, &stop, &all_thread_stats[i]);
    }

    std::this_thread::sleep_for(std::chrono::seconds(kTestDurationSec));
    stop.store(true);
    for (auto& t : workers) t.join();

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

  run_benchmark(kDeltaDBPath, true, "DELTA MODE");
  // 70w
  run_benchmark(kNativeDBPath, false, "NATIVE MODE");

  return 0;
}
