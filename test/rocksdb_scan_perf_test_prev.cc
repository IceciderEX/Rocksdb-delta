/**
 * rocksdb_perf_test.cc
 *
 * RocksDB-delta 性能测试工具
 * 用于对比原生 RocksDB 与 Delta 优化方案在 PK 模型下的表现。
 *
 * 场景模拟：
 * 1. 入库负载：顺序写入 + 范围扫描 (Partial Scan)
 * 2. 查询负载：全表扫描 (Scan)，包含 15/77 热点分布
 * 3. 生命周期：Put -> Scan -> Put -> Scan -> ScanAll -> DeleteAll
 */

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

using namespace ROCKSDB_NAMESPACE;

// 配置常量
const std::string kNativeDBPath = "/home/wam/Rocksdb-delta/db_perf_test/db_perf_native";
const std::string kDeltaDBPath = "/home/wam/Rocksdb-delta/db_perf_test/db_perf_delta";
const int kNumCuids = 500;           // 总 CUID 数量
const int kRowsPerCuid = 10000;        // 每个 CUID 初始行数
const double kHotRatio = 0.15;        // 15% 的热点
const double kHotAccessRatio = 0.77;  // 77% 的访问集中在热点

bool CleanupDBPath(const std::string& path) {
  std::error_code ec;
  if (path != kNativeDBPath && path != kDeltaDBPath) {
    std::cerr << "Refusing to delete non-test path: " << path << std::endl;
    return false;
  }
  std::filesystem::remove_all(path, ec);
  if (ec) {
    std::cerr << "Failed to cleanup path " << path << ": " << ec.message()
              << std::endl;
    return false;
  }
  return true;
}

// ==========================================
// 辅助工具函数
// ==========================================
std::string GenerateKey(uint64_t cuid, int row_id) {
  std::string key;
  key.resize(40);
  std::memset(&key[0], 0, 40);

  unsigned char* p = reinterpret_cast<unsigned char*>(&key[0]) + 16;
  for (int i = 0; i < 8; ++i) {
    p[i] = (cuid >> (56 - 8 * i)) & 0xFF;
  }

  char row_buf[16];
  snprintf(row_buf, sizeof(row_buf), "%010d", row_id);
  std::memcpy(&key[24], row_buf, 10);

  return key;
}

uint64_t ExtractCUID(const Slice& key) {
  if (key.size() < 24) return 0;
  const unsigned char* p =
      reinterpret_cast<const unsigned char*>(key.data()) + 16;
  uint64_t c = 0;
  for (int i = 0; i < 8; ++i) {
    c = (c << 8) | p[i];
  }
  return c;
}

struct Stats {
  long total_ops = 0;
  long total_rows = 0;
  double total_time_ms = 0;

  void Report(const std::string& name) {
    double avg_latency = total_ops > 0 ? total_time_ms / total_ops : 0;
    double throughput_rows =
        total_time_ms > 0 ? (total_rows / (total_time_ms / 1000.0)) : 0;
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "[" << name << "] Ops: " << total_ops
              << ", Avg Latency: " << avg_latency << " ms"
              << ", Throughput: " << throughput_rows << " rows/s" << std::endl;
  }
};

// ==========================================
// 核心测试逻辑
// ==========================================

class PerfTester {
 public:
  PerfTester(DB* db) : db_(db) {
    // 初始化热点 CUID 列表
    int num_hot = static_cast<int>(kNumCuids * kHotRatio);
    for (int i = 0; i < num_hot; ++i) {
      hot_cuids_.push_back(i + 1);
    }
    for (int i = num_hot; i < kNumCuids; ++i) {
      cold_cuids_.push_back(i + 1);
    }
    current_rows_per_cuid_.resize(kNumCuids + 1, kRowsPerCuid);
  }

  // 1. 基准初始化 (冷数据入库)
  void PrepareData() {
    std::cout << "Preparing initial data (" << kNumCuids << " CUIDs, "
              << kRowsPerCuid << " rows each)..." << std::endl;
    for (int i = 0; i < kNumCuids; ++i) {
      WriteBatch batch;
      uint64_t cuid = i + 1;
      for (int r = 0; r < kRowsPerCuid; ++r) {
        batch.Put(GenerateKey(cuid, r), "val_" + std::to_string(r));
      }
      db_->Write(WriteOptions(), &batch);
    }
    std::cout << "Data prep finished and flushed." << std::endl;
  }

  // 2. 预热 (触发 Delta 判定)
  void WarmUp() {
    std::cout << "Warming up hot CUIDs to trigger hotspot detection..."
              << std::endl;
    ReadOptions ro;
    ro.delta_full_scan = true;  // 预热阶段必须扫描全表触发判定
    for (uint64_t cuid : hot_cuids_) {
      for (int i = 0; i < 5; ++i) {
        ScanCUID(cuid, ro);
      }
    }
  }

  // 3. 运行工作负载 A: 入库负载 (Write + Sequential Scan)
  Stats RunWorkloadA(bool use_delta) {
    Stats stats;
    ReadOptions ro;
    ro.delta_full_scan = !use_delta;

    std::cout << "\nRunning Workload A (Ingestion-Heavy, Delta="
              << (use_delta ? "ON" : "OFF") << ")..." << std::endl;

    // 增加循环次数到 500，且每次写入更多数据 (500行)
    for (int i = 0; i < 500; ++i) {
      uint64_t cuid = hot_cuids_[i % hot_cuids_.size()];
      int start_row = current_rows_per_cuid_[cuid];

      // Step A: Put 500 rows
      WriteBatch batch;
      for (int r = 0; r < 500; ++r) {
        batch.Put(GenerateKey(cuid, start_row + r), "new_val");
      }
      db_->Write(WriteOptions(), &batch);

      // Step B: Partial Scan
      auto start = std::chrono::high_resolution_clock::now();
      int rows = ScanCUIDRange(cuid, start_row, start_row + 499, ro);
      auto end = std::chrono::high_resolution_clock::now();

      current_rows_per_cuid_[cuid] += 500;

      stats.total_ops++;
      stats.total_rows += rows;
      stats.total_time_ms +=
          std::chrono::duration<double, std::milli>(end - start).count();
    }
    return stats;
  }

  // 4. 运行工作负载 B: 查询负载 (Point/Range Scans with Skew)
  Stats RunWorkloadB(bool use_delta, int total_scans) {
    Stats stats;
    ReadOptions ro;
    ro.delta_full_scan = !use_delta;

    std::default_random_engine generator;
    std::uniform_real_distribution<double> distribution(0.0, 1.0);

    std::cout << "\nRunning Workload B (Query-Heavy/Skewed, Delta="
              << (use_delta ? "ON" : "OFF") << ")..." << std::endl;

    for (int i = 0; i < total_scans; ++i) {
      uint64_t cuid;
      if (distribution(generator) < kHotAccessRatio) {
        cuid = hot_cuids_[rand() % hot_cuids_.size()];
      } else {
        cuid = cold_cuids_[rand() % cold_cuids_.size()];
      }

      auto start = std::chrono::high_resolution_clock::now();
      int rows = ScanCUID(cuid, ro);
      auto end = std::chrono::high_resolution_clock::now();

      stats.total_ops++;
      stats.total_rows += rows;
      stats.total_time_ms +=
          std::chrono::duration<double, std::milli>(end - start).count();
    }
    return stats;
  }

  // 5. 生命周期负载 (Put -> Scan -> Put -> Scan -> ScanAll -> DeleteAll)
  Stats RunWorkloadC(bool use_delta) {
    Stats stats;
    ReadOptions ro;
    ro.delta_full_scan = !use_delta;

    std::cout << "\nRunning Workload C (Lifecycle Simulation, Delta="
              << (use_delta ? "ON" : "OFF") << ")..." << std::endl;

    // 增加循环次数到 100，模拟更多实体的完整生命周期
    for (int i = 0; i < 100; ++i) {
      uint64_t cuid = hot_cuids_[i % hot_cuids_.size()];
      int start_row = current_rows_per_cuid_[cuid];
      auto start = std::chrono::high_resolution_clock::now();

      // Step 1: Put (Append 2000 rows)
      WriteBatch batch;
      for (int r = 0; r < 2000; ++r) {
        batch.Put(GenerateKey(cuid, start_row + r), "append_val");
      }
      db_->Write(WriteOptions(), &batch);
      current_rows_per_cuid_[cuid] += 2000;

      // Step 2 & 3: Multiple Scans (Triggers Partial Merge)
      for (int s = 0; s < 5; ++s) {
        // 扫新增数据以及部分历史数据
        int scan_start = std::max(0, start_row - 100);
        ScanCUIDRange(cuid, scan_start, current_rows_per_cuid_[cuid] - 1, ro);
      }

      // Step 4: Full Scan
      int rows = ScanCUID(cuid, ro);

      // Step 5: Clean up (Individual Delete per key)
      // 处理该实体的所有物理行
      for (int r = 0; r < current_rows_per_cuid_[cuid]; ++r) {
        db_->Delete(WriteOptions(), GenerateKey(cuid, r));
      }
      current_rows_per_cuid_[cuid] = 0;

      auto end = std::chrono::high_resolution_clock::now();

      stats.total_ops++;
      stats.total_rows += rows;
      stats.total_time_ms +=
          std::chrono::duration<double, std::milli>(end - start).count();
    }
    return stats;
  }

 private:
  int ScanCUID(uint64_t cuid, const ReadOptions& ro) {
    std::string start_key = GenerateKey(cuid, 0);
    std::string end_key = GenerateKey(cuid + 1, 0);
    Slice upper_bound(end_key);
    ReadOptions ro_copy = ro;
    ro_copy.iterate_upper_bound = &upper_bound;

    Iterator* it = db_->NewIterator(ro_copy);
    int count = 0;
    for (it->Seek(start_key); it->Valid(); it->Next()) {
      if (ExtractCUID(it->key()) != cuid) break;
      count++;
    }
    delete it;
    return count;
  }

  int ScanCUIDRange(uint64_t cuid, int start_row, int end_row,
                    const ReadOptions& ro) {
    std::string start_key = GenerateKey(cuid, start_row);
    std::string end_key = GenerateKey(cuid, end_row + 1);
    Slice upper_bound(end_key);
    ReadOptions ro_copy = ro;
    ro_copy.iterate_upper_bound = &upper_bound;

    Iterator* it = db_->NewIterator(ro_copy);
    int count = 0;
    for (it->Seek(start_key); it->Valid(); it->Next()) {
      if (ExtractCUID(it->key()) != cuid) break;
      count++;
    }
    delete it;
    return count;
  }

  DB* db_;
  std::vector<uint64_t> hot_cuids_;
  std::vector<uint64_t> cold_cuids_;
  std::vector<int> current_rows_per_cuid_;
};

int main() {
  if (!CleanupDBPath(kNativeDBPath) || !CleanupDBPath(kDeltaDBPath)) {
    return 1;
  }

  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;

  // --- 1. Native 模式测试 (纯净基准) ---
  std::cout << ">>> STARTING NATIVE MODE BENCHMARK <<<" << std::endl;
  options.enable_delta = false;  // 彻底禁用 Delta 逻辑
  DestroyDB(kNativeDBPath, options);
  DB* native_db;
  Status s = DB::Open(options, kNativeDBPath, &native_db);
  if (!s.ok()) {
    std::cerr << "Open Native DB failed: " << s.ToString() << std::endl;
    return 1;
  }

  PerfTester native_tester(native_db);
  native_tester.PrepareData();
  // Native 模式不需要 WarmUp

  Stats native_a = native_tester.RunWorkloadA(false);
  Stats native_b = native_tester.RunWorkloadB(false, 50);
  Stats native_c = native_tester.RunWorkloadC(false);

  delete native_db;

  // --- 2. Delta 模式测试 (优化方案) ---
  std::cout << "\n>>> STARTING DELTA MODE BENCHMARK <<<" << std::endl;
  options.enable_delta = true;  // 开启 Delta 优化
  DestroyDB(kDeltaDBPath, options);
  DB* delta_db;
  s = DB::Open(options, kDeltaDBPath, &delta_db);
  if (!s.ok()) {
    std::cerr << "Open Delta DB failed: " << s.ToString() << std::endl;
    return 1;
  }

  PerfTester delta_tester(delta_db);
  delta_tester.PrepareData();
  delta_tester.WarmUp();  // 预热以收集热点信息

  Stats delta_a = delta_tester.RunWorkloadA(true);
  Stats delta_b = delta_tester.RunWorkloadB(true, 50);
  Stats delta_c = delta_tester.RunWorkloadC(true);

  delete delta_db;

  // --- 3. 结果汇总 ---
  std::cout << "\n================ RESULT SUMMARY ================"
            << std::endl;
  native_a.Report("Native Ingestion-Heavy");
  delta_a.Report("Delta  Ingestion-Heavy");
  std::cout << "------------------------------------------------" << std::endl;
  native_b.Report("Native Query-Heavy");
  delta_b.Report("Delta  Query-Heavy");
  std::cout << "------------------------------------------------" << std::endl;
  native_c.Report("Native Lifecycle");
  delta_c.Report("Delta  Lifecycle");
  std::cout << "================================================" << std::endl;

  return 0;
}
