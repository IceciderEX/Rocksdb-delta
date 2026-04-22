#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "delta/hot_data_buffer.h"
#include "delta/hot_index_table.h"
#include "delta/hotspot_manager.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;

const std::string kDBPath = "db_tmp_deep_test";

void Check(bool condition, const std::string& msg) {
  if (condition) {
    std::cout << "[PASS] " << msg << std::endl;
  } else {
    std::cerr << "[FAIL] " << msg << std::endl;
    exit(1);
  }
}

// 辅助方法：生成固定长度的 Key (CUID + RowID)
std::string GenerateKey(uint64_t cuid, uint64_t row_id) {
  std::string key(40, '\0');
  unsigned char* p = reinterpret_cast<unsigned char*>(&key[16]);
  for (int i = 0; i < 8; ++i) {
    p[i] = (cuid >> (56 - 8 * i)) & 0xFF;
  }
  unsigned char* q = reinterpret_cast<unsigned char*>(&key[32]);
  for (int i = 0; i < 8; ++i) {
    q[i] = (row_id >> (56 - 8 * i)) & 0xFF;
  }
  return key;
}

// 提取 CUID
uint64_t ExtractCUID(const std::string& key) {
  if (key.size() < 24) return 0;
  uint64_t cuid = 0;
  const unsigned char* p =
      reinterpret_cast<const unsigned char*>(key.data() + 16);
  for (int i = 0; i < 8; ++i) {
    cuid = (cuid << 8) | p[i];
  }
  return cuid;
}

// 写入数据并 Flush (模拟底层 Cold Data 生成 SST)
void WriteBatchAndFlush(DB* db, uint64_t cuid, uint64_t start_row, int count) {
  WriteOptions write_opts;
  for (int i = 0; i < count; ++i) {
    std::string key = GenerateKey(cuid, start_row + i);
    std::string value = "val_" + std::to_string(start_row + i);
    Status s = db->Put(write_opts, key, value);
    if (!s.ok()) {
      std::cerr << "Put failed: " << s.ToString() << std::endl;
      exit(1);
    }
  }
  FlushOptions flush_opts;
  db->Flush(flush_opts);
}

// 执行全量扫描，模拟查询
int PerformFullScan(DB* db, uint64_t cuid, bool cold_path = false) {
  ReadOptions read_opts;
  read_opts.delta_full_scan = true;
  read_opts.skip_hot_path = cold_path;

  std::string start_key = GenerateKey(cuid, 0);
  std::string upper_bound_key = GenerateKey(cuid + 1, 0);

  Slice start_slice(start_key);
  Slice upper_bound_slice(upper_bound_key);
  read_opts.iterate_upper_bound = &upper_bound_slice;

  std::unique_ptr<Iterator> iter(db->NewIterator(read_opts));
  int count = 0;
  for (iter->Seek(start_slice); iter->Valid(); iter->Next()) {
    count++;
  }
  if (!iter->status().ok() && !iter->status().IsNotFound()) {
    std::cerr << "Full scan error: " << iter->status().ToString() << std::endl;
  }
  return count;
}

// 执行部分扫描，模拟范围查询
int PerformPartialScan(DB* db, uint64_t cuid, uint64_t start_row,
                       uint64_t end_row) {
  ReadOptions read_opts;
  read_opts.delta_full_scan = false;
  read_opts.skip_hot_path = false;

  std::string start_key = GenerateKey(cuid, start_row);
  std::string upper_bound_key = GenerateKey(cuid, end_row + 1);

  Slice start_slice(start_key);
  Slice upper_bound_slice(upper_bound_key);
  read_opts.iterate_upper_bound = &upper_bound_slice;

  std::unique_ptr<Iterator> iter(db->NewIterator(read_opts));
  int count = 0;
  for (iter->Seek(start_slice); iter->Valid(); iter->Next()) {
    count++;
  }
  return count;
}

int main() {
  std::cout << "========================================" << std::endl;
  std::cout << "RocksDB Delta Architecture Comprehensive Test Suite"
            << std::endl;
  std::cout << "========================================" << std::endl;

  Options options;
  options.create_if_missing = true;
  DestroyDB(kDBPath, options);

  options.create_missing_column_families = true;
  options.disable_auto_compactions = true;
  options.num_levels = 1;
  options.level0_file_num_compaction_trigger = 20;
  options.level_compaction_dynamic_level_bytes = false;

  DB* db = nullptr;
  Status s = DB::Open(options, kDBPath, &db);
  Check(s.ok(), "DB Open");

  DBImpl* db_impl = dynamic_cast<DBImpl*>(db);
  auto hotspot_mgr = db_impl->GetHotspotManager();
  Check(hotspot_mgr != nullptr, "HotspotManager Access");

  const uint64_t TEST_CUID = 8888;

  // ------------------------------------------------------------------
  std::cout << "\n>>> TEST 1: Write Initial Cold Data (SST Generation) <<<\n";
  // 写入两批打底数据 (Cold Data) 并 Flush 成底层的 SST。
  WriteBatchAndFlush(db, TEST_CUID, 100, 50);  // row [100, 149]
  WriteBatchAndFlush(db, TEST_CUID, 200, 50);  // row [200, 249]
  std::cout << "Initial cold SSTs generated." << std::endl;

  int cold_rows = PerformFullScan(db, TEST_CUID, true);  // cold path
  Check(cold_rows == 100, "Cold scan found identical 100 rows.");

  // ------------------------------------------------------------------
  std::cout
      << "\n>>> TEST 2: Hotspot Detection & Initial Scan-as-Compaction <<<\n";
  // 多次执行全量扫描，直到跨越热点阈值 (5次)。由于之前没缓存，首次需要建
  // Snapshot
  for (int i = 0; i < 5; ++i) {
    PerformFullScan(db, TEST_CUID, false);
  }
  Check(hotspot_mgr->IsHot(TEST_CUID), "CUID promoted to HOT.");

  // 后台消费初始化扫描队列，它会通过 Cold Path 抽取前面的底层 100 行到内存
  // Buffer
  if (hotspot_mgr->HasPendingInitCuids()) {
    db_impl->ProcessPendingHotCuids();
  }

  HotIndexEntry entry;
  Check(hotspot_mgr->GetIndexTable().GetEntry(TEST_CUID, &entry),
        "HotIndexTable tracks CUID.");
  Check(entry.HasSnapshot(),
        "Initial Snapshot (-1 segment) is created accurately.");
  Check(PerformFullScan(db, TEST_CUID, false) == 100,
        "Hot Path read visibility is maintained.");

  // ------------------------------------------------------------------
  std::cout << "\n>>> TEST 3: In-Memory Delta Appending <<<\n";
  // CUID变热以后，新的写入将会生成 Delta 片段。
  WriteBatchAndFlush(db, TEST_CUID, 300, 20);  // row [300, 319]
  WriteBatchAndFlush(db, TEST_CUID, 330, 20);  // row [330, 349]
  WriteBatchAndFlush(db, TEST_CUID, 360, 20);  // row [360, 379]
  WriteBatchAndFlush(db, TEST_CUID, 390, 20);  // row [390, 409]
  WriteBatchAndFlush(db, TEST_CUID, 420, 20);  // row [420, 439]
  WriteBatchAndFlush(db, TEST_CUID, 450, 20);  // row [450, 469]

  hotspot_mgr->GetIndexTable().GetEntry(TEST_CUID, &entry);
  Check(entry.deltas.size() == 6,
        "Delta SSTs mapped and appended into HotIndexTable correctly.");
  Check(PerformFullScan(db, TEST_CUID, false) == 220,
        "Hot Scan seamlessly integrates buffers and active deltas.");

  // ------------------------------------------------------------------
  std::cout << "\n>>> TEST 4: Partial Scan triggering Delta Coalescing <<<\n";
  // 部分扫描区间 [300, 365] 会包含前三个 Delta，触发 Partial Merge
  int p_rows = PerformPartialScan(db, TEST_CUID, 300, 365);
  std::cout << "Partial scan matched rows: " << p_rows << std::endl;

  if (hotspot_mgr->HasPendingPartialMerge()) {
    std::cout << "Executing background ProcessPendingPartialMerge..."
              << std::endl;
    db_impl->ProcessPendingPartialMerge();
  }

  hotspot_mgr->GetIndexTable().GetEntry(TEST_CUID, &entry);
  Check(entry.obsolete_deltas.size() >= 3,
        "[300,319], [330,349] and [360,379] SSTs were moved to obsolete_deltas "
        "due to "
        "overlapping merge.");
  Check(PerformFullScan(db, TEST_CUID, false) == 220,
        "Data integrity maintained after internal Coalescing.");

  // ------------------------------------------------------------------
  std::cout << "\n>>> TEST 5: Background Metadata Scan (GDCT Update) <<<\n";
  // EVS 或缓存需要更新。我们会做 metadata update
  hotspot_mgr->EnqueueMetadataScan(TEST_CUID);
  db_impl->ProcessPendingMetadataScans();
  std::cout << "Metadata scan processed." << std::endl;

  // ------------------------------------------------------------------
  std::cout << "\n>>> TEST 6: L0 Compaction & Obsolete Delta Purge <<<\n";
  // 使用 rocksdb 的 manual compaction
  CompactRangeOptions compact_options;
  db->CompactRange(compact_options, nullptr, nullptr);

  hotspot_mgr->GetIndexTable().GetEntry(TEST_CUID, &entry);
  Check(entry.obsolete_deltas.empty(),
        "Obsolete deltas list successfully purged after L0 compaction.");

  int final_rows = PerformFullScan(db, TEST_CUID, false);
  std::cout << "Post-compaction total rows: " << final_rows << std::endl;
  Check(final_rows == 220,
        "Zero data loss! Full L0 Compaction data recycling works flawlessly.");

  std::cout << "\n========================================" << std::endl;
  std::cout << "All RocksDB Delta deep tests passed successfully!" << std::endl;
  std::cout << "========================================" << std::endl;

  delete db;
  return 0;
}
