#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "delta/hotspot_manager.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;

namespace {

const std::string kDBPath = "/home/jx/Rocksdb-delta/db_tmp";
// <=0 means run without built-in time limit until externally stopped.
const int kTestDurationSec = 0;
const int kWorkerThreads = 8;
const int kBatchSize = 128;
const int kTargetPutBatches = 200;      // 25600 rows per CUID
const int kSmallScanWindow = 500;
const int kSmallScanEveryNBatches = 2;
const bool kVerboseWorkerLog = false;
const bool kEnableDeltaFullScan = false;

std::atomic<bool> stop_test{false};
std::atomic<uint64_t> next_cuid{1001};

struct TestStats {
  std::atomic<uint64_t> total_writes{0};
  std::atomic<uint64_t> total_small_scans{0};
  std::atomic<uint64_t> total_full_scans{0};
  std::atomic<uint64_t> total_deletes{0};
  std::atomic<uint64_t> total_lifecycle_completed{0};
  std::atomic<uint64_t> total_merges{0};
  std::atomic<uint64_t> errors{0};
};

TestStats global_stats;

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

uint64_t ExtractRowID(const Slice& key) {
  if (key.size() < 34) return 0;
  std::string row_str = key.ToString().substr(24, 10);
  return std::stoull(row_str);
}

bool ScanRows(DB* db, uint64_t cuid, int begin_row, int end_row_exclusive,
              bool delta_full_scan, size_t* out_count,
              std::vector<uint64_t>* out_rows) {
  if (begin_row < 0 || end_row_exclusive < begin_row) {
    return false;
  }

  ReadOptions ro;
  ro.delta_full_scan = delta_full_scan;

  std::string start_key = GenerateKey(cuid, begin_row);
  std::string upper_key = GenerateKey(cuid, end_row_exclusive);
  Slice upper_slice = upper_key;
  ro.iterate_upper_bound = &upper_slice;

  std::unique_ptr<Iterator> it(db->NewIterator(ro));
  size_t count = 0;
  if (out_rows != nullptr) {
    out_rows->clear();
  }

  for (it->Seek(start_key); it->Valid(); it->Next()) {
    if (ExtractCUID(it->key()) != cuid) break;
    ++count;
    if (out_rows != nullptr) {
      out_rows->push_back(ExtractRowID(it->key()));
    }
  }

  if (!it->status().ok()) {
    return false;
  }

  if (out_count != nullptr) {
    *out_count = count;
  }
  return true;
}

bool VerifySmallScan(DB* db, uint64_t cuid, int written_rows, int worker_id,
                     bool delta_full_scan) {
  int begin_row = std::max(0, written_rows - kSmallScanWindow);
  int end_row = written_rows;
  const size_t expected = static_cast<size_t>(end_row - begin_row);

  size_t actual = 0;
  std::vector<uint64_t> rows;
  if (!ScanRows(db, cuid, begin_row, end_row, delta_full_scan, &actual, &rows)) {
    std::cerr << "Worker " << worker_id
              << " small-scan failed for CUID " << cuid << std::endl;
    return false;
  }

  if (actual != expected) {
    std::cerr << "Worker " << worker_id << " small-scan count mismatch for CUID "
              << cuid << ", expected=" << expected << ", actual=" << actual
              << std::endl;
    return false;
  }

  for (size_t i = 0; i < rows.size(); ++i) {
    uint64_t want = static_cast<uint64_t>(begin_row) + i;
    if (rows[i] != want) {
      std::cerr << "Worker " << worker_id
                << " small-scan row mismatch for CUID " << cuid
                << ", expected row=" << want << ", actual row=" << rows[i]
                << std::endl;
      return false;
    }
  }

  global_stats.total_small_scans++;
  return true;
}

bool VerifyFullScan(DB* db, uint64_t cuid, int written_rows, int worker_id,
                    bool delta_full_scan) {
  size_t actual = 0;
  std::vector<uint64_t> rows;
  if (!ScanRows(db, cuid, 0, written_rows, delta_full_scan, &actual, &rows)) {
    std::cerr << "Worker " << worker_id
              << " full-scan failed for CUID " << cuid << std::endl;
    return false;
  }

  if (actual != static_cast<size_t>(written_rows)) {
    std::cerr << "Worker " << worker_id << " full-scan count mismatch for CUID "
              << cuid << ", expected=" << written_rows << ", actual=" << actual
              << std::endl;
    return false;
  }

  for (size_t i = 0; i < rows.size(); ++i) {
    if (rows[i] != i) {
      std::cerr << "Worker " << worker_id
                << " full-scan row mismatch for CUID " << cuid
                << ", expected row=" << i << ", actual row=" << rows[i]
                << std::endl;
      return false;
    }
  }

  global_stats.total_full_scans++;
  return true;
}

bool DeleteAllRows(DB* db, uint64_t cuid, int written_rows, int worker_id) {
  WriteOptions wo;
  for (int start = 0; start < written_rows; start += kBatchSize) {
    int end = std::min(written_rows, start + kBatchSize);
    WriteBatch wb;
    for (int row = start; row < end; ++row) {
      wb.Delete(GenerateKey(cuid, row));
    }

    Status s = db->Write(wo, &wb);
    if (!s.ok()) {
      std::cerr << "Worker " << worker_id << " delete batch failed for CUID "
                << cuid << ", range=[" << start << "," << end
                << "), status=" << s.ToString() << std::endl;
      return false;
    }
    global_stats.total_deletes += static_cast<uint64_t>(end - start);
  }

  Status fs = db->Flush(FlushOptions());
  if (!fs.ok()) {
    std::cerr << "Worker " << worker_id << " flush after delete failed for CUID "
              << cuid << ", status=" << fs.ToString() << std::endl;
    return false;
  }
  return true;
}

bool VerifyEmptyAfterDelete(DB* db, uint64_t cuid, int worker_id) {
  size_t actual = 0;
  if (!ScanRows(db, cuid, 0, 1, false, &actual, nullptr)) {
    std::cerr << "Worker " << worker_id
              << " empty-scan failed for CUID " << cuid << std::endl;
    return false;
  }

  if (actual != 0) {
    std::cerr << "Worker " << worker_id
              << " empty-scan found residual rows for CUID " << cuid
              << ", actual=" << actual << std::endl;
    return false;
  }
  return true;
}

void LifecycleWorker(DB* db, int worker_id) {
  WriteOptions wo;
  while (!stop_test) {
    const uint64_t cuid = next_cuid.fetch_add(1);
    int written_rows = 0;

    // Stage A: write + small scans interleaving.
    for (int b = 0; b < kTargetPutBatches && !stop_test; ++b) {
      WriteBatch wb;
      for (int i = 0; i < kBatchSize; ++i) {
        wb.Put(GenerateKey(cuid, written_rows + i), "val");
      }

      Status s = db->Write(wo, &wb);
      if (!s.ok()) {
        std::cerr << "Worker " << worker_id << " write failed for CUID " << cuid
                  << ", batch=" << b << ", status=" << s.ToString() << std::endl;
        global_stats.errors++;
        goto next_cuid_cycle;
      }

      written_rows += kBatchSize;
      global_stats.total_writes += kBatchSize;

      if ((b + 1) % kSmallScanEveryNBatches == 0) {
        if (!VerifySmallScan(db, cuid, written_rows, worker_id,
                             kEnableDeltaFullScan)) {
          global_stats.errors++;
          goto next_cuid_cycle;
        }
      }
    }

    // Stage B: full scan.
    if (!stop_test &&
        !VerifyFullScan(db, cuid, written_rows, worker_id, kEnableDeltaFullScan)) {
      global_stats.errors++;
      goto next_cuid_cycle;
    }

    // Stage C: full delete.
    if (!stop_test && !DeleteAllRows(db, cuid, written_rows, worker_id)) {
      global_stats.errors++;
      goto next_cuid_cycle;
    }

    // Stage D: empty verification scan.
    if (!stop_test && !VerifyEmptyAfterDelete(db, cuid, worker_id)) {
      global_stats.errors++;
      goto next_cuid_cycle;
    }

    global_stats.total_lifecycle_completed++;

  next_cuid_cycle:
    if (kVerboseWorkerLog && !stop_test) {
      std::cout << "Worker " << worker_id << " finished lifecycle for CUID "
                << cuid << std::endl;
    }
  }
}

void ManagerThread(DBImpl* db_impl) {
  auto hotspot_mgr = db_impl->GetHotspotManager();
  while (!stop_test) {
    if (hotspot_mgr->HasPendingInitCuids()) {
      db_impl->ProcessPendingHotCuids();
    }
    if (hotspot_mgr->HasPendingPartialMerge()) {
      db_impl->ProcessPendingPartialMerge();
      global_stats.total_merges++;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
  }
}

}  // namespace

int main() {
  stop_test = false;
  next_cuid = 1001;
  global_stats.total_writes.store(0);
  global_stats.total_small_scans.store(0);
  global_stats.total_full_scans.store(0);
  global_stats.total_deletes.store(0);
  global_stats.total_lifecycle_completed.store(0);
  global_stats.total_merges.store(0);
  global_stats.errors.store(0);

  Options options;
  options.create_if_missing = true;
  options.enable_delta = true;
  options.delta_options.enable_partition = true;

  options.delta_options.hotspot_scan_threshold = 200;
  options.delta_options.hotspot_scan_window_sec = 300;
  options.delta_options.delta_merge_threshold = 3;
  options.delta_options.sac_delta_count_threshold = 5;
  options.delta_options.sharding_count = 64;
  options.delta_options.hot_data_buffer_threshold_bytes = 64 * 1024 * 1024;
  options.delta_options.hot_data_buffer_shards = 128;
  options.delta_options.compaction_l0_trigger_count = 20;
  options.delta_options.compaction_l0_trigger_age_sec = 3600;
  options.delta_options.compaction_l0_files_to_pick = 10;
  options.num_levels = 1;
  options.level0_file_num_compaction_trigger = 20;
  options.level_compaction_dynamic_level_bytes = false;

  DestroyDB(kDBPath, options);

  DB* db = nullptr;
  Status s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) {
    std::cerr << "Open failed: " << s.ToString() << std::endl;
    return 1;
  }

  DBImpl* db_impl = static_cast<DBImpl*>(db);

  if (kTestDurationSec > 0) {
    std::cout << "Starting lifecycle stress test for " << kTestDurationSec
              << " seconds..." << std::endl;
  } else {
    std::cout << "Starting lifecycle stress test with NO time limit..."
              << std::endl;
  }
  std::cout << "Model: write+small-scan -> full-scan -> full-delete -> empty-scan"
            << std::endl;

  std::vector<std::thread> workers;
  workers.reserve(kWorkerThreads);
  for (int i = 0; i < kWorkerThreads; ++i) {
    workers.emplace_back(LifecycleWorker, db, i + 1);
  }
  std::thread manager(ManagerThread, db_impl);

  auto start_time = std::chrono::steady_clock::now();
  while (kTestDurationSec <= 0 ||
         std::chrono::steady_clock::now() - start_time <
             std::chrono::seconds(kTestDurationSec)) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cout << "Stats: Writes=" << global_stats.total_writes
              << ", SmallScans=" << global_stats.total_small_scans
              << ", FullScans=" << global_stats.total_full_scans
              << ", Deletes=" << global_stats.total_deletes
              << ", Lifecycles=" << global_stats.total_lifecycle_completed
              << ", Merges=" << global_stats.total_merges
              << ", Errors=" << global_stats.errors << std::endl;
  }

  stop_test = true;
  for (auto& t : workers) {
    t.join();
  }
  manager.join();

  std::cout << "Lifecycle stress test completed." << std::endl;
  uint64_t final_errors = global_stats.errors;
  if (final_errors > 0) {
    std::cout << "Test FAILED with " << final_errors << " errors." << std::endl;
  } else {
    std::cout << "Test PASSED." << std::endl;
  }

  delete db;
  return final_errors == 0 ? 0 : 1;
}