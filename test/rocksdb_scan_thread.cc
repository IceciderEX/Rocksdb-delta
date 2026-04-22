#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <random>
#include <string>
#include <thread>
#include <filesystem>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "delta/hotspot_manager.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"

using namespace ROCKSDB_NAMESPACE;

const std::string kDBPath = "/home/wam/Rocksdb-delta/db_tmp";
std::atomic<bool> stop_test{false};

struct TestStats {
  std::atomic<uint64_t> total_writes{0};
  std::atomic<uint64_t> total_scans{0};
  std::atomic<uint64_t> total_merges{0};
  std::atomic<uint64_t> errors{0};
};

TestStats global_stats;

// Ground truth per CUID to verify data integrity
struct CuidGroundTruth {
  std::mutex mtx;
  std::set<uint64_t> row_ids;
};
std::map<uint64_t, CuidGroundTruth*> ground_truths;
// [FIX] Global mutex to protect access to ground_truths map itself (not just the contents)
std::mutex ground_truths_mtx;

std::string GenerateKey(uint64_t cuid, int row_id) {
  std::string key;
  key.resize(40);
  std::memset(&key[0], 0, 40);

  unsigned char* p = reinterpret_cast<unsigned char*>(&key[0]) + 16;
  for (int i = 0; i < 8; ++i) {
    p[i] = (cuid >> (56 - 8 * i)) & 0xFF;
  }

  // 使用固定10位宽度格式，确保字典序=数字序
  // "123" -> "0000000123"
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

std::string FormatKeyDisplay(const Slice& key) {
  std::string cuid_part =
      std::to_string(key.size() >= 24 ? ExtractCUID(key) : 0);
  std::string suffix = key.size() > 24 ? key.ToString().substr(24) : "";
  return cuid_part + "..." + suffix;
}

void WriterThread(DB* db, const std::vector<uint64_t>& cuids) {
  uint64_t next_rid = 0;
  uint64_t target_cuid = cuids[rand() % cuids.size()];
  WriteOptions wo;
  while (!stop_test) {
    WriteBatch batch;
    uint64_t batch_start_rid = next_rid;
    int batch_size = 1024;

    {
      std::lock_guard<std::mutex> gt_lock(ground_truths_mtx);
      auto gt_it = ground_truths.find(target_cuid);
      if (gt_it == ground_truths.end()) continue;
      std::lock_guard<std::mutex> lock(gt_it->second->mtx);
      for (int k = 0; k < batch_size; ++k) {
        uint64_t rid = next_rid++;
        batch.Put(GenerateKey(target_cuid, rid),
                  "val_xxxxxxxxxxxxxxxx_" + std::to_string(rid));
      }
    }
    // 必须先写 DB，再更新 ground_truths，否则 VerifyThread 可能看到行
    // 已在期望集合中但 DB 尚未写入，导致误报 missing rows
    db->Write(wo, &batch);
    {
      std::lock_guard<std::mutex> gt_lock(ground_truths_mtx);
      auto gt_it = ground_truths.find(target_cuid);
      if (gt_it == ground_truths.end()) continue;
      std::lock_guard<std::mutex> lock(gt_it->second->mtx);
      for (uint64_t rid = batch_start_rid; rid < next_rid; ++rid) {
        gt_it->second->row_ids.insert(rid);
      }
    }
    global_stats.total_writes += batch_size;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

void ReaderThread(DB* db, const std::vector<uint64_t>& cuids, int id) {
  ReadOptions ro;
  // 仅让其中一个 Reader 输出调试日志，避免多线程日志堆叠
  if (id == 1) {
    ro.enable_delta_diag_logging = true;
  }

  std::mt19937 gen(id + static_cast<uint32_t>(time(0)));
  while (!stop_test) {
    uint64_t cuid = cuids[gen() % cuids.size()];
    ro.delta_full_scan = false;

    // 获取当前数据的总量
    size_t cur_total_rows = 0;
    {
      std::lock_guard<std::mutex> gt_lock(ground_truths_mtx);
      auto gt_it = ground_truths.find(cuid);
      if (gt_it == ground_truths.end()) continue;
      std::lock_guard<std::mutex> lock(gt_it->second->mtx);
      cur_total_rows = gt_it->second->row_ids.size();
    }

    if (cur_total_rows == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    // 随机扫描 10%-50% 的数据范围
    uint64_t scan_len = 0;
    uint64_t start_row = 0;
    
    if (cur_total_rows < 10) {
      scan_len = cur_total_rows;
      start_row = 0;
    } else {
      uint64_t min_len = static_cast<uint64_t>(cur_total_rows * 0.1);
      uint64_t max_len = static_cast<uint64_t>(cur_total_rows * 0.5);
      std::uniform_int_distribution<uint64_t> len_dist(min_len, std::max(min_len + 1, max_len));
      scan_len = len_dist(gen);

      std::uniform_int_distribution<uint64_t> start_dist(0, cur_total_rows - scan_len);
      start_row = start_dist(gen);
    }

    uint64_t end_row = start_row + scan_len;

    if (ro.enable_delta_diag_logging) {
      std::cout << "[Reader " << id << "] Starting scan for CUID " << cuid
                << ", range [" << start_row << ", " << end_row 
                << "), len=" << scan_len << ", total=" << cur_total_rows << std::endl;
    }

    std::string start_key = GenerateKey(cuid, start_row);
    std::string upper_bound = GenerateKey(cuid, end_row);
    Slice ub_slice = upper_bound;
    ro.iterate_upper_bound = &ub_slice;

    // Take snapshot of expected rows before starting the scan, filtered by range
    std::set<uint64_t> expected;
    {
      std::lock_guard<std::mutex> gt_lock(ground_truths_mtx);
      auto gt_it = ground_truths.find(cuid);
      if (gt_it == ground_truths.end()) continue;
      std::lock_guard<std::mutex> lock(gt_it->second->mtx);
      auto it_low = gt_it->second->row_ids.lower_bound(start_row);
      auto it_high = gt_it->second->row_ids.lower_bound(end_row);
      for (auto it = it_low; it != it_high; ++it) {
        expected.insert(*it);
      }
    }

    std::unique_ptr<Iterator> it(db->NewIterator(ro));
    std::set<uint64_t> found;
    for (it->Seek(start_key); it->Valid(); it->Next()) {
      if (ExtractCUID(it->key()) != cuid) break;
      found.insert(ExtractRowID(it->key()));
    }

    if (!it->status().ok()) {
      std::cerr << "Reader " << id << " error: " << it->status().ToString()
                << std::endl;
      global_stats.errors++;
    } else {
      std::vector<uint64_t> missing;
      for (uint64_t rid : expected) {
        if (found.find(rid) == found.end()) {
          missing.push_back(rid);
        }
      }

      if (!missing.empty() && ro.enable_delta_diag_logging) {
        if (missing.size() <= 10) {
          for (uint64_t rid : missing) {
            std::cerr << "Reader " << id << " error: Missing row " << rid
                      << " for cuid " << cuid << std::endl;
          }
        } else {
          for (int i = 0; i < 3; i++) {
            std::cerr << "Reader " << id << " error: Missing row " << missing[i]
                      << " for cuid " << cuid << std::endl;
          }
          std::cerr << "Reader " << id << " error: ... (skipped "
                    << (missing.size() - 40) << " entries) ..." << std::endl;
          for (size_t i = missing.size() - 3; i < missing.size(); i++) {
            std::cerr << "Reader " << id << " error: Missing row " << missing[i]
                      << " for cuid " << cuid << std::endl;
          }
        }
        std::cerr << "Reader " << id
                  << " error: Total missing rows: " << missing.size()
                  << " for cuid " << cuid << ", found=" << found.size()
                  << ", expected=" << expected.size() << std::endl;
        global_stats.errors += missing.size();

        // Diagnostic: dump HotIndexEntry state
        auto hotspot_mgr = dynamic_cast<DBImpl*>(db)->GetHotspotManager();
        if (hotspot_mgr && ro.enable_delta_diag_logging) {
          HotIndexEntry diag_entry;
          if (hotspot_mgr->GetHotIndexEntry(cuid, &diag_entry)) {
            std::cerr << "[DIAG] CUID " << cuid << " snapshot_segments="
                      << diag_entry.snapshot_segments.size()
                      << " deltas=" << diag_entry.deltas.size() << std::endl;
            for (size_t si = 0; si < diag_entry.snapshot_segments.size();
                 si++) {
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
          } else {
            std::cerr << "[DIAG] CUID " << cuid << " has NO HotIndexEntry!"
                      << std::endl;
          }

          int count = 0;
          int mod = 0;
          {
            std::lock_guard<std::mutex> gt_lock(ground_truths_mtx);
            auto gt_it = ground_truths.find(cuid);
            if (gt_it != ground_truths.end()) {
              std::lock_guard<std::mutex> lock(gt_it->second->mtx);
              mod = gt_it->second->row_ids.size() / 10;
            }
          }
          std::unique_ptr<Iterator> it2(db->NewIterator(ro));
          for (it2->Seek(start_key); it2->Valid(); it2->Next()) {
            if (ExtractCUID(it2->key()) != cuid) break;
            if (count % mod == 0)
              std::cout << "Reader " << id << ": "
                        << FormatKeyDisplay(it2->key()) << std::endl;
            found.insert(ExtractRowID(it2->key()));
            count++;
          }
          int i = 0;
          count = i;
        } 
      } else if (gen() % 100 < 5 && ro.enable_delta_diag_logging) {
        // Log diagnostic information for a small percentage of scans
        auto hotspot_mgr = dynamic_cast<DBImpl*>(db)->GetHotspotManager();
        HotIndexEntry diag_entry;
        if (hotspot_mgr->GetHotIndexEntry(cuid, &diag_entry)) {
          std::cerr << "[DIAG] CUID " << cuid << " snapshot_segments="
                    << diag_entry.snapshot_segments.size()
                    << " deltas=" << diag_entry.deltas.size() << std::endl;
          for (size_t si = 0; si < diag_entry.snapshot_segments.size();
                si++) {
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
        } else {
          std::cerr << "[DIAG] CUID " << cuid << " has NO HotIndexEntry!"
                    << std::endl;
        }
      }
    }
    global_stats.total_scans++;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
}

// 每隔 60 秒对每个 CUID 执行全量 scan，验证所有行均可见。
// 使用 is_metadata_scan=true：走热点路径（读 delta/buffer）但不触发 ScanAsCompaction。
void VerifyThread(DB* db, const std::vector<uint64_t>& cuids) {
  while (!stop_test) {
    // 等待 60 秒，每秒检查一次 stop_test
    for (int i = 0; i < 60 && !stop_test; i++) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (stop_test) break;

    std::cout << "[VERIFY] Starting full-range integrity check for all CUIDs..."
              << std::endl;

    for (uint64_t cuid : cuids) {
      // 获取当前期望行的快照（取有序 vector 以便确定扫描范围）
      std::vector<uint64_t> expected_vec;
      {
        std::lock_guard<std::mutex> gt_lock(ground_truths_mtx);
        auto gt_it = ground_truths.find(cuid);
        if (gt_it == ground_truths.end()) continue;
        std::lock_guard<std::mutex> lock(gt_it->second->mtx);
        expected_vec.assign(gt_it->second->row_ids.begin(),
                            gt_it->second->row_ids.end());
      }
      if (expected_vec.empty()) continue;

      uint64_t start_row = expected_vec.front();
      uint64_t end_row   = expected_vec.back() + 1;  // upper_bound exclusive

      std::string start_key   = GenerateKey(cuid, start_row);
      std::string upper_bound = GenerateKey(cuid, end_row);
      Slice ub_slice(upper_bound);

      ReadOptions ro;
      ro.is_metadata_scan = true;  // 走热点路径但不触发 ScanAsCompaction
      ro.delta_full_scan        = false;
      ro.enable_delta_diag_logging = false;
      ro.iterate_upper_bound    = &ub_slice;

      std::set<uint64_t> found;
      std::unique_ptr<Iterator> it(db->NewIterator(ro));
      for (it->Seek(start_key); it->Valid(); it->Next()) {
        if (ExtractCUID(it->key()) != cuid) break;
        found.insert(ExtractRowID(it->key()));
      }

      int index = 0;
      if (!it->status().ok()) {
        std::cerr << "[VERIFY] CUID " << cuid
                  << " scan error: " << it->status().ToString() << std::endl;
        global_stats.errors++;
        continue;
      }

      // 比对：找出所有期望存在但实际未读到的行
      std::vector<uint64_t> missing;
      {
        std::lock_guard<std::mutex> gt_lock(ground_truths_mtx);
        auto gt_it = ground_truths.find(cuid);
        if (gt_it != ground_truths.end()) {
          std::lock_guard<std::mutex> lock(gt_it->second->mtx);
          for (uint64_t rid : expected_vec) {
            if (found.find(rid) == found.end()) {
              missing.push_back(rid);
            }
          }
        }
      }

      if (!missing.empty()) {
        std::cerr << "[VERIFY] *** CUID " << cuid << ": " << missing.size()
                  << " MISSING rows! range=[" << start_row << "," << end_row
                  << ") expected=" << expected_vec.size()
                  << " found=" << found.size() << std::endl;
        size_t report_n = std::min(missing.size(), (size_t)5);
        for (size_t i = 0; i < report_n; i++) {
          std::cerr << "[VERIFY]   missing rid=" << missing[i] << std::endl;
        }
        // 输出最后 report_n 条数据
        for (size_t i = missing.size() > report_n ? missing.size() - report_n : 0; i < missing.size(); i++) {
          std::cerr << "[VERIFY]   missing rid=" << missing[i] << std::endl;
        }
        if (missing.size() > 2 * report_n) {
          std::cerr << "[VERIFY]   ... and " << (missing.size() - 5)
                    << " more missing rows" << std::endl;
        }
        auto hotspot_mgr = dynamic_cast<DBImpl*>(db)->GetHotspotManager();
        if (hotspot_mgr) {
          HotIndexEntry diag_entry;
          if (hotspot_mgr->GetHotIndexEntry(cuid, &diag_entry)) {
            std::cerr << "[VERIFY] CUID " << cuid
                      << " snapshot_segments="
                      << diag_entry.snapshot_segments.size()
                      << " deltas=" << diag_entry.deltas.size() << std::endl;
            for (size_t si = 0; si < diag_entry.snapshot_segments.size();
                 ++si) {
              const auto& seg = diag_entry.snapshot_segments[si];
              std::cerr << "[VERIFY]   snap[" << si
                        << "] file=" << static_cast<int64_t>(seg.file_number)
                        << " first_key=" << FormatKeyDisplay(seg.first_key)
                        << " last_key=" << FormatKeyDisplay(seg.last_key)
                        << std::endl;
            }
            for (size_t di = 0; di < diag_entry.deltas.size(); ++di) {
              const auto& seg = diag_entry.deltas[di];
              std::cerr << "[VERIFY]   delta[" << di
                        << "] file=" << static_cast<int64_t>(seg.file_number)
                        << " first_key=" << FormatKeyDisplay(seg.first_key)
                        << " last_key=" << FormatKeyDisplay(seg.last_key)
                        << std::endl;
            }
          } else {
            std::cerr << "[VERIFY] CUID " << cuid << " has NO HotIndexEntry!"
                      << std::endl;
          }
        }
        
        global_stats.errors += missing.size();
      } else {
        std::cout << "[VERIFY] CUID " << cuid << ": OK  "
                  << found.size() << " rows verified." << std::endl;
      }
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

bool CleanupDBPath(const std::string& path) {
  std::error_code ec;
  if (path != kDBPath) {
    std::cerr << "Refusing to delete non-test path: " << path << std::endl;
    return false;
  }
  std::filesystem::remove_all(path, ec);
  return true;
}

int main() {
  const std::string kDeltaDBPath = "/home/wam/Rocksdb-delta/db_tmp";
  CleanupDBPath(kDBPath);  

  Options options;
  options.create_if_missing = true;
  options.enable_delta = true;
  options.write_buffer_size = 16 * 1024 * 1024;
  options.target_file_size_base = 16 * 1024 * 1024;

  // --- Example 1: Programmatic Configuration of DeltaOptions ---
  // These can be set directly on the options object before opening the DB.
  options.delta_options.hotspot_scan_threshold = 3;
  options.delta_options.hotspot_scan_window_sec = 300;
  options.delta_options.delta_merge_threshold = 3;
  options.delta_options.sac_delta_count_threshold = 5;
  options.delta_options.sharding_count = 64;  // Power of 2 recommended
  options.delta_options.hot_data_buffer_threshold_bytes = 16 * 1024 * 1024;
  options.delta_options.hot_data_buffer_shards = 128;
  options.delta_options.compaction_l0_trigger_count = 20;
  options.delta_options.compaction_l0_trigger_age_sec = 3600;
  options.delta_options.compaction_l0_files_to_pick = 10;
  // -------------------------------------------------------------
  options.level0_slowdown_writes_trigger = 1000;     // l0 file count thres
  options.level0_stop_writes_trigger = 2000;         // l0 file count thres
  options.level0_file_num_compaction_trigger = 100;  // l0 file count thres
  options.soft_pending_compaction_bytes_limit = 0;   // 0 表示无限制
  options.hard_pending_compaction_bytes_limit = 0;   // 0 表示无限制
  options.num_levels = 1;
  options.level0_file_num_compaction_trigger = 20;
  options.level_compaction_dynamic_level_bytes = false;
  // DestroyDB(kDBPath, options);

  DB* db = nullptr;
  Status s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) {
    std::cerr << "Open failed: " << s.ToString() << std::endl;
    return 1;
  }

  DBImpl* db_impl = dynamic_cast<DBImpl*>(db);
  std::vector<uint64_t> cuids = {1001, 1002, 1003, 1004, 1005};
  for (uint64_t cuid : cuids) {
    ground_truths[cuid] = new CuidGroundTruth();
  }

  std::cout << "Starting Deep Stress Test for 90000 seconds..." << std::endl;

  std::thread writer1(WriterThread, db, cuids);
  std::thread writer2(WriterThread, db, cuids);
  std::thread writer3(WriterThread, db, cuids);
  std::thread reader1(ReaderThread, db, cuids, 1);
  std::thread reader2(ReaderThread, db, cuids, 2);
  std::thread reader3(ReaderThread, db, cuids, 3);
  std::thread reader4(ReaderThread, db, cuids, 4);
  std::thread verifier(VerifyThread, db, cuids);
  std::thread manager(ManagerThread, db_impl);

  auto start_time = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - start_time <
         std::chrono::seconds(90000)) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cout << "Stats: Writes=" << global_stats.total_writes
              << ", Scans=" << global_stats.total_scans
              << ", Merges=" << global_stats.total_merges
              << ", Errors=" << global_stats.errors << std::endl;
  }

  stop_test = true;
  writer1.join();
  writer2.join();
  writer3.join();
  reader1.join();
  reader2.join();
  reader3.join();
  reader4.join();
  verifier.join();
  manager.join();

  std::cout << "Stress Test Completed." << std::endl;
  uint64_t final_errors = global_stats.errors;
  if (final_errors > 0) {
    std::cout << "Test FAILED with " << final_errors << " errors." << std::endl;
  } else {
    std::cout << "Test PASSED." << std::endl;
  }

  for (auto& pair : ground_truths) {
    delete pair.second;
  }
  delete db;
  return (final_errors == 0) ? 0 : 1;
}
