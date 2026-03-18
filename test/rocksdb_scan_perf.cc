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
const std::string kNativeDBPath = "/home/wam/Rocksdb-delta/db_perf_test/db_perf_native";
const std::string kDeltaDBPath = "/home/wam/Rocksdb-delta/db_perf_test/db_perf_delta";
const int kNumThreads = 16;
const int kTestDurationSec = 480;       // 8 分钟
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
  PerfRunner(rocksdb::DB* db, std::atomic<uint64_t>* next_cuid) 
      : db_(db), next_cuid_(next_cuid) {}

  void Run(int thread_id, std::atomic<bool>* stop, ThreadStats* stats) {
    std::default_random_engine gen(thread_id + static_cast<int>(time(0)));
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    rocksdb::ReadOptions read_opts;

    while (!stop->load()) {
      // 1. 领取一个全新的 CUID
      uint64_t cuid = next_cuid_->fetch_add(1);
      if (cuid > kNumCuids) break;

      // 2. 状态初始化
      // 使用简单的哈希分散热点分布，避免热动 CUID 集中顺序出现
      bool is_hot = (((cuid * 1103515245 + 12345) / 65536) % 100 < (kHotRatio * 100));
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
          DoPut(cuid, puts_done * kBatchSize, stats);
          puts_done++;
        } else {
          bool full_scan = (dist(gen) < 0.1);
          DoScan(cuid, puts_done * kBatchSize, full_scan, read_opts, stats);
          scans_done++;
        }
      }

      // 4. 终结清理：最后一次 Full Scan -> 逐一物理删除
      int final_rows = puts_done * kBatchSize;
      DoScan(cuid, final_rows, true, read_opts, stats);
      DoDelete(cuid, final_rows, stats);

      std::cout << "[Thread " << thread_id << "] Completed CUID " << cuid
                << " (Rows: " << final_rows << ", Scans: " << scans_done
                << ", Hot: " << is_hot << ")" << std::endl;
    }
  }

 private:
  void DoPut(uint64_t cuid, int start_row, ThreadStats* stats) {
    rocksdb::WriteBatch batch;
    for (int i = 0; i < kBatchSize; ++i) {
      batch.Put(GenerateKey(cuid, start_row + i), "value_data_payload_xxxxxxx");
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
    
    if (delta_enabled) {
      options.enable_delta = true;
      options.disable_auto_compactions = true;
      options.num_levels = 1;
      options.level0_file_num_compaction_trigger = 20;
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
    delete db;
  };

  run_benchmark(kDeltaDBPath, true, "DELTA MODE");
  run_benchmark(kNativeDBPath, false, "NATIVE MODE");

  return 0;
}
