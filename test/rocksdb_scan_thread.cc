#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "delta/hotspot_manager.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;

const std::string kDBPath = "/home/wam/Rocksdb-delta/db_tmp";
std::atomic<bool> stop_test{false};
constexpr int kPutIntervalSeconds = 5;
constexpr int kScanIntervalSeconds = 5;

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
  std::string cuid_part = std::to_string(key.size() >= 24 ? ExtractCUID(key) : 0);
  std::string suffix = key.size() > 24 ? key.ToString().substr(24) : "";
  return cuid_part + "..." + suffix;
}

void WriterThread(DB* db, const std::vector<uint64_t>& cuids) {
  uint64_t next_row_per_cuid[10] = {0};
  WriteOptions wo;
  while (!stop_test) {
    for (size_t i = 0; i < cuids.size(); ++i) {
      uint64_t cuid = cuids[i];
      {
        std::lock_guard<std::mutex> lock(ground_truths[cuid]->mtx);
        for (int k = 0; k < 100; ++k) {
          uint64_t rid = next_row_per_cuid[i]++;
          db->Put(wo, GenerateKey(cuid, rid), "val_xxxxxxxxxxxxxxxxx");
          ground_truths[cuid]->row_ids.insert(rid);
        }
      }
      // db->Flush(FlushOptions());
      global_stats.total_writes += 100;
    }
    std::this_thread::sleep_for(std::chrono::seconds(kPutIntervalSeconds));
  }
}

void ReaderThread(DB* db, const std::vector<uint64_t>& cuids, int id) {
  ReadOptions ro;
  while (!stop_test) {
    uint64_t cuid = cuids[rand() % cuids.size()];
    ro.delta_full_scan = (rand() % 2 == 0);
    std::cout << "[Reader " << id << "] Starting scan for CUID " << cuid
              << ", delta_full_scan=" << ro.delta_full_scan << std::endl;

    std::string start_key = GenerateKey(cuid, 0);
    std::string upper_bound = GenerateKey(cuid + 1, 0);
    Slice ub_slice = upper_bound;
    ro.iterate_upper_bound = &ub_slice;

    // Take snapshot of expected rows before starting the scan
    std::set<uint64_t> expected;
    {
      std::lock_guard<std::mutex> lock(ground_truths[cuid]->mtx);
      expected = ground_truths[cuid]->row_ids;
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

      if (!missing.empty()) {
        if (missing.size() <= 10) {
          for (uint64_t rid : missing) {
            std::cerr << "Reader " << id << " error: Missing row " << rid
                      << " for cuid " << cuid << std::endl;
          }
        } else {
          for (int i = 0; i < 5; i++) {
            std::cerr << "Reader " << id << " error: Missing row " << missing[i]
                      << " for cuid " << cuid << std::endl;
          }
          std::cerr << "Reader " << id << " error: ... (skipped "
                    << (missing.size() - 40) << " entries) ..." << std::endl;
          for (size_t i = missing.size() - 5; i < missing.size(); i++) {
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
        if (hotspot_mgr) {
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

          // Check buffer data count
          // auto* buf_iter = hotspot_mgr->NewBufferIterator(cuid, nullptr);
          // if (buf_iter) {
          //   size_t buf_count = 0;
          //   for (buf_iter->SeekToFirst(); buf_iter->Valid(); buf_iter->Next()) {
          //     buf_count++;
          //   }
          //   std::cerr << "[DIAG] Buffer data count for CUID " << cuid << ": "
          //             << buf_count << std::endl;
          //   delete buf_iter;
          // }
          

          // int count = 0;
          // std::unique_ptr<Iterator> it2(db->NewIterator(ro));
          // for (it2->Seek(start_key); it2->Valid(); it2->Next()) {
          //   if (ExtractCUID(it2->key()) != cuid) break;
          //   if (count % 100 == 0) std::cout << "Reader " << id << ": " << FormatKeyDisplay(it2->key()) << std::endl;
          //   found.insert(ExtractRowID(it2->key()));
          //   count++;
          // }
          // int i = 0;
          // count = i;
        }
      } 
      else {
        // if (rand() % 100 < 5) {
        //   auto hotspot_mgr = dynamic_cast<DBImpl*>(db)->GetHotspotManager();
        //   if (hotspot_mgr) {
        //     HotIndexEntry diag_entry;
        //     if (hotspot_mgr->GetHotIndexEntry(cuid, &diag_entry)) {
        //       std::cerr << "[DIAG] CUID " << cuid << " snapshot_segments="
        //                 << diag_entry.snapshot_segments.size()
        //                 << " deltas=" << diag_entry.deltas.size() << std::endl;
        //       for (size_t si = 0; si < diag_entry.snapshot_segments.size();
        //           si++) {
        //         const auto& seg = diag_entry.snapshot_segments[si];
        //         std::cerr << "[DIAG]   snap[" << si
        //                   << "] file=" << (int64_t)seg.file_number
        //                   << " first_key=" << FormatKeyDisplay(seg.first_key)
        //                   << " last_key=" << FormatKeyDisplay(seg.last_key)
        //                   << std::endl;
        //       }
        //       for (size_t di = 0; di < diag_entry.deltas.size(); di++) {
        //         const auto& seg = diag_entry.deltas[di];
        //         std::cerr << "[DIAG]   delta[" << di
        //                   << "] file=" << (int64_t)seg.file_number
        //                   << " first_key=" << FormatKeyDisplay(seg.first_key)
        //                   << " last_key=" << FormatKeyDisplay(seg.last_key)
        //                   << std::endl;
        //       }
        //     } else {
        //       std::cerr << "[DIAG] CUID " << cuid << " has NO HotIndexEntry!"
        //                 << std::endl;
        //     }
        //   }
        // }
      }
    } 
    global_stats.total_scans++;
    std::this_thread::sleep_for(std::chrono::seconds(kScanIntervalSeconds));
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

int main() {
  Options options;
  options.create_if_missing = true;
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
  options.delta_options.compaction_l0_trigger_count = 20;
  options.delta_options.compaction_l0_trigger_age_sec = 3600;
  options.delta_options.compaction_l0_files_to_pick = 10;
  // -------------------------------------------------------------
  options.level0_slowdown_writes_trigger = 1000; // l0 file count thres
  options.level0_stop_writes_trigger = 2000; // l0 file count thres
  options.level0_file_num_compaction_trigger = 100; // l0 file count thres
  options.soft_pending_compaction_bytes_limit = 0; // 0 表示无限制
  options.hard_pending_compaction_bytes_limit = 0; // 0 表示无限制
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

  DBImpl* db_impl = dynamic_cast<DBImpl*>(db);
  std::vector<uint64_t> cuids = {1001, 1002, 1003, 1004, 1005};
  for (uint64_t cuid : cuids) {
    ground_truths[cuid] = new CuidGroundTruth();
  }

  std::cout << "Starting Deep Stress Test for 30 seconds..." << std::endl;

  std::thread writer(WriterThread, db, cuids);
  std::thread reader1(ReaderThread, db, cuids, 1);
  std::thread reader2(ReaderThread, db, cuids, 2);
  std::thread reader3(ReaderThread, db, cuids, 3);
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
  writer.join();
  reader1.join();
  reader2.join();
  // reader3.join();
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