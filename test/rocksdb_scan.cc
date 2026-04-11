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

// ==========================================
// 配置常量
// ==========================================
const std::string kNativeDBPath = "/home/wam/Rocksdb-delta/db_perf_test/db_perf_native";
const std::string kDeltaDBPath = "/home/wam/Rocksdb-delta/db_perf_test/db_perf_delta";
const int kNumThreads = 32;
const int kTestDurationSec = 3600;       // s
const int kNumCuids = 1000000;           // 100W CUID 总库
const int kBatchSize = 512;             // 每次 Put 512 行
const int kTargetPutBatches = 120;      // 每个 CUID 固定写入 200 个 batch (目标约 6W 行)
const double kHotRatio = 0.15;          // 15% 的热点
const int kHotScanTarget = 100;        // 热点访问目标
const int kColdScanTarget = 20;        // 普通访问目标

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

uint64_t ExtractCUID(const rocksdb::Slice& key) {
  if (key.size() < 24) return 0;
  const unsigned char* p = reinterpret_cast<const unsigned char*>(key.data()) + 16;
  uint64_t c = 0;
  for (int i = 0; i < 8; ++i) {
    c = (c << 8) | p[i];
  }
  return c;
}

std::string FormatKeyDisplay(const rocksdb::Slice& key) {
  std::string cuid_part =
      std::to_string(key.size() >= 24 ? ExtractCUID(key) : 0);
  std::string suffix = key.size() > 24 ? key.ToString().substr(24) : "";
  return cuid_part + "..." + suffix;
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
      std::chrono::steady_clock::time_point last_scan_time = std::chrono::steady_clock::now();
    };

  void Run(int thread_id, std::atomic<bool>* stop, ThreadStats* stats) {
    std::default_random_engine gen(thread_id + static_cast<int>(time(0)));
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    rocksdb::ReadOptions read_opts;
    
    std::vector<CuidState> active_cuids;
    std::deque<std::pair<uint64_t, int>> pending_deletes;
    
    // === 100GB 目标数据量计算过程 ===
    // 单个 CUID 数据量约：128 batch * 500 rows/batch * 72 bytes/row ≈ 4.3945 MB
    // 实现 100GB 常驻数据需 100,000 / 4.3945 ≈ 22,750 个 CUID
    // 设 32 线程，则每线程需维持 22,750 / 32 ≈ 711 个 CUID (活跃 + 待删除)
    const size_t kMaxActiveCuids = 50;
    const size_t kDeleteWindowSize = 150; 

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


      // 3. 执行动作
      if (do_put) {
        DoPut(state.cuid, state.puts_done * kBatchSize, stats);
        state.puts_done++;
      } else {
        bool full_scan = false; // Full Scan SAC 机制已移除
        DoScan(state.cuid, state.puts_done * kBatchSize, full_scan, read_opts, stats, gen, thread_id);
        state.scans_done++;
        // state.last_scan_time = now;
      }

      // 模拟业务节奏
      // std::this_thread::sleep_for(std::chrono::milliseconds(5));

      // 4. 判断该 CUID 是否终于走完了完整的轮回
      if (state.puts_done >= kTargetPutBatches && state.scans_done >= state.target_scans) {
        int final_rows = state.puts_done * kBatchSize;
        int final_scan_count = DoScan(state.cuid, final_rows, true, read_opts, stats, gen, thread_id);
        if (final_scan_count != final_rows) {
          std::cerr << "[ERROR] Thread " << thread_id << " - CUID " << state.cuid 
                    << " final full scan mismatch! Expected: " << final_rows 
                    << ", Actual: " << final_scan_count << std::endl;

          std::vector<int> missing_rows;
          rocksdb::ReadOptions diag_ro = read_opts;
          diag_ro.delta_full_scan = true;
          rocksdb::Iterator* diag_it = db_->NewIterator(diag_ro);
          diag_it->Seek(GenerateKey(state.cuid, 0));
          int current_expected = 0;
          while (diag_it->Valid() && ExtractCUID(diag_it->key()) == state.cuid) {
            std::string key_str = diag_it->key().ToString();
            int row_id = std::stoi(key_str.substr(24, 10)); // 从 key_str 中提取 row_id
            while (current_expected < row_id && current_expected < final_rows) {
              missing_rows.push_back(current_expected);
              current_expected++;
            }
            current_expected = std::max(current_expected, row_id + 1);
            diag_it->Next();
          }
          while (current_expected < final_rows) {
            missing_rows.push_back(current_expected);
            current_expected++;
          }
          delete diag_it;

          if (!missing_rows.empty()) {
            std::cerr << "[DIAG] Missing row_ids count: " << missing_rows.size() << "\n[DIAG] First missing row_ids: ";
            for (size_t i = 0; i < std::min<size_t>(5, missing_rows.size()); i++) {
              std::cerr << missing_rows[i] << " ";
            }
            std::cerr << "\n";
            if (missing_rows.size() > 5) {
              std::cerr << "[DIAG] Last missing row_ids: ";
              size_t start_idx = std::max<size_t>(5, missing_rows.size() - 5);
              for (size_t i = start_idx; i < missing_rows.size(); i++) {
                std::cerr << missing_rows[i] << " ";
              }
              std::cerr << "\n";
            }
          }
          
          auto hotspot_mgr = static_cast<rocksdb::DBImpl*>(db_)->GetHotspotManager();
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
                          << " first_key=" << FormatKeyDisplay(seg.first_key)
                          << " last_key=" << FormatKeyDisplay(seg.last_key)
                          << std::endl;
              }
              for (size_t di = 0; di < diag_entry.deltas.size(); di++) {
                const auto& seg = diag_entry.deltas[di];
                std::cerr << "[DIAG]   delta[" << di
                          << "] file=" << (int64_t)seg.file_number
                          << " first_key=" << FormatKeyDisplay(seg.first_key)
                          << " last_key=" << FormatKeyDisplay(seg.last_key)
                          << std::endl;
              }
            }
          }
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

  int DoScan(uint64_t cuid, int cur_rows, bool full_scan, const rocksdb::ReadOptions& ro,
             ThreadStats* stats, std::default_random_engine& gen, int thread_id) {
    std::string start_key, end_key;
    int expected_start_row = 0;
    int expected_count = 0;

    if (full_scan) {
      start_key = GenerateKey(cuid, 0);
      end_key = GenerateKey(cuid + 1, 0);
      expected_start_row = 0;
      expected_count = cur_rows;
    } else {
      // 随机扫描 10%-50% 的数据范围
      if (cur_rows < 10) {
        start_key = GenerateKey(cuid, 0);
        end_key = GenerateKey(cuid, cur_rows);
        expected_start_row = 0;
        expected_count = cur_rows;
      } else {
        int min_len = static_cast<int>(cur_rows * 0.1);
        int max_len = static_cast<int>(cur_rows * 0.5);
        std::uniform_int_distribution<int> len_dist(min_len, std::max(min_len + 1, max_len));
        int scan_len = len_dist(gen);

        std::uniform_int_distribution<int> start_dist(0, std::max(0, cur_rows - scan_len));
        int start_row = start_dist(gen);
        start_key = GenerateKey(cuid, start_row);
        end_key = GenerateKey(cuid, start_row + scan_len);
        expected_start_row = start_row;
        expected_count = scan_len;
      }
    }

    rocksdb::Slice upper_bound(end_key);
    rocksdb::ReadOptions ro_copy = ro;
    ro_copy.iterate_upper_bound = &upper_bound;
    ro_copy.delta_full_scan = false;  // 用户扫描不再触发 SAC，仅由后台 Init Scan 负责

    auto t_start = std::chrono::high_resolution_clock::now();
    rocksdb::Iterator* it = db_->NewIterator(ro_copy);
    it->Seek(start_key);

    int count = 0;
    int expected_row = expected_start_row;
    while (it->Valid()) {
      if (ExtractCUID(it->key()) != cuid) break;

      // 行 ID 连续性检查
      std::string key_str = it->key().ToString();
      int actual_row = std::stoi(key_str.substr(24, 10));
      if (actual_row != expected_row) {
        int gap_start_row = expected_row;
        int gap_end_row   = actual_row;
        fprintf(stderr,
                "[SCAN_GAP] Thread %d CUID %lu: expected row %d, got row %d "
                "(missing %d rows). full_scan=%d expected_range=[%d,%d)\n",
                thread_id, (unsigned long)cuid,
                gap_start_row, gap_end_row, gap_end_row - gap_start_row,
                (int)full_scan, expected_start_row,
                expected_start_row + expected_count);

        // ── 诊断①：冷路径验证 gap 范围内数据是否存在于 RocksDB ──
        {
          std::string cold_start = GenerateKey(cuid, gap_start_row);
          std::string cold_end   = GenerateKey(cuid, gap_end_row);
          rocksdb::Slice cold_upper(cold_end);
          rocksdb::ReadOptions cold_ro_diag;
          cold_ro_diag.skip_hot_path = true;   // 绕过热路径，走原生 RocksDB
          cold_ro_diag.delta_full_scan = false;
          cold_ro_diag.iterate_upper_bound = &cold_upper;
          rocksdb::Iterator* cold_it = db_->NewIterator(cold_ro_diag);
          cold_it->Seek(cold_start);
          int cold_count = 0;
          while (cold_it->Valid() && ExtractCUID(cold_it->key()) == cuid) {
            cold_count++;
            cold_it->Next();
          }
          delete cold_it;
          fprintf(stderr,
                  "[SCAN_GAP_DIAG] Cold path sees %d/%d rows in gap [%d,%d). "
                  "→ %s\n",
                  cold_count, gap_end_row - gap_start_row,
                  gap_start_row, gap_end_row,
                  cold_count > 0
                    ? "Data IN RocksDB but INVISIBLE to hot path (memtable not flushed yet)"
                    : "Data NOT in RocksDB (write failure or premature delete)");
        }

        // ── 诊断②：热点索引状态 ──
        auto hotspot_mgr =
            static_cast<rocksdb::DBImpl*>(db_)->GetHotspotManager();
        if (hotspot_mgr) {
          bool is_hot = hotspot_mgr->IsHot(cuid);
          rocksdb::HotIndexEntry diag_entry;
          bool has_entry = hotspot_mgr->GetHotIndexEntry(cuid, &diag_entry);
          fprintf(stderr,
                  "[SCAN_GAP_DIAG] CUID %lu is_hot=%d has_index=%d "
                  "snaps=%zu deltas=%zu\n",
                  (unsigned long)cuid, (int)is_hot, (int)has_entry,
                  has_entry ? diag_entry.snapshot_segments.size() : 0,
                  has_entry ? diag_entry.deltas.size() : 0);
          if (has_entry) {
            for (size_t si = 0; si < diag_entry.snapshot_segments.size(); si++) {
              const auto& seg = diag_entry.snapshot_segments[si];
              fprintf(stderr,
                      "[SCAN_GAP_DIAG]   snap[%zu] file=%ld [%s - %s]\n",
                      si, (int64_t)seg.file_number,
                      FormatKeyDisplay(rocksdb::Slice(seg.first_key)).c_str(),
                      FormatKeyDisplay(rocksdb::Slice(seg.last_key)).c_str());
            }
            for (size_t di = 0; di < diag_entry.deltas.size(); di++) {
              const auto& seg = diag_entry.deltas[di];
              fprintf(stderr,
                      "[SCAN_GAP_DIAG]   delta[%zu] file=%ld [%s - %s]\n",
                      di, (int64_t)seg.file_number,
                      FormatKeyDisplay(rocksdb::Slice(seg.first_key)).c_str(),
                      FormatKeyDisplay(rocksdb::Slice(seg.last_key)).c_str());
            }
          }
        }

        expected_row = actual_row + 1;  // 重新对齐，继续检查后续行
      } else {
        expected_row++;
      }
      count++;
      it->Next();
    }
    delete it;
    auto t_end = std::chrono::high_resolution_clock::now();
    stats->AddScan(std::chrono::duration<double, std::milli>(t_end - t_start).count(), count);

    // 总行数检查（cur_rows==0 时跳过，刚开始写入时 expected_count 为 0 是正常的）
    if (expected_count > 0 && count != expected_count) {
      fprintf(stderr,
              "[SCAN_COUNT] Thread %d CUID %lu: count mismatch, "
              "expected %d got %d full_scan=%d range=[%d,%d)\n",
              thread_id, (unsigned long)cuid, expected_count, count,
              (int)full_scan, expected_start_row,
              expected_start_row + expected_count);
    }

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
      options.delta_options.hotspot_scan_threshold = 5;
      options.delta_options.hotspot_scan_window_sec = 300;
      options.delta_options.delta_merge_threshold = 6;
      options.delta_options.sac_delta_count_threshold = 12;
      options.delta_options.sharding_count = 64; // Power of 2 recommended
      options.delta_options.hot_data_buffer_threshold_bytes = 64 * 1024 * 1024;
      options.delta_options.hot_data_buffer_shards = 128;
      options.delta_options.compaction_l0_trigger_count = 30;
      options.delta_options.compaction_l0_trigger_age_sec = 3600;
      options.delta_options.compaction_l0_files_to_pick = 10;
      options.delta_options.max_delta_threads = 8;
      // -------------------------------------------------------------
      options.level0_slowdown_writes_trigger = 200; // l0 file count thres
      options.level0_stop_writes_trigger = 400; // l0 file count thres
      options.level0_file_num_compaction_trigger = 30; // l0 file count thres
      options.max_subcompactions = 4; // subcompaction 线程数
      options.soft_pending_compaction_bytes_limit = 0; // 0 表示无限制
      options.hard_pending_compaction_bytes_limit = 0; // 0 表示无限制
      options.max_background_jobs = 32; // 与写入线程相同？
      options.num_levels = 1;
      options.level_compaction_dynamic_level_bytes = false;
    } else {
      options.enable_delta = false;
      options.max_subcompactions = 4;
      options.max_background_jobs = 32;
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
  run_benchmark(kNativeDBPath, false, "NATIVE MODE");

  return 0;
}
