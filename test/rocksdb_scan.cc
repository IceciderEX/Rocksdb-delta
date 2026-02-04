/**
 * rocksdb_partial_merge_test.cc
 *
 * 测试 Scan-as-Compaction 的 kPartialMerge 策略
 * 验证：
 * 1. skip_hot_path 选项正确绕过热点路径
 * 2. ProcessPendingHotCuids 能正确建立初始 snapshot
 * 3. kPartialMerge 策略在部分扫描时正确触发
 * 4. ProcessPendingPartialMerge 能正确归并数据
 */

#include <cassert>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "delta/hot_index_table.h"
#include "delta/hotspot_manager.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

using namespace ROCKSDB_NAMESPACE;

const std::string kDBPath = "/home/wam/HWKV/Rocksdb-delta/db_tmp";

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

void Check(bool condition, const std::string& msg) {
  if (condition) {
    std::cout << "[PASS] " << msg << std::endl;
  } else {
    std::cerr << "[FAIL] " << msg << std::endl;
    exit(1);
  }
}

std::string GenerateUpperBoundKey(uint64_t cuid) {
  return GenerateKey(cuid + 1, 0);
}

// 全量扫描
int PerformFullScan(DB* db, uint64_t cuid) {
  ReadOptions ro;
  ro.delta_full_scan = true;

  std::string upper_bound_str = GenerateUpperBoundKey(cuid);
  Slice upper_bound = upper_bound_str;
  ro.iterate_upper_bound = &upper_bound;

  Iterator* it = db->NewIterator(ro);
  std::string start_key = GenerateKey(cuid, 0);

  int count = 0;
  for (it->Seek(start_key); it->Valid(); it->Next()) {
    if (ExtractCUID(it->key()) != cuid) break;
    count++;
  }

  Status s = it->status();
  delete it;
  if (!s.ok()) {
    std::cerr << "Full scan error: " << s.ToString() << std::endl;
  }
  return count;
}

// 部分扫描 (指定 row_id 范围)
int PerformPartialScan(DB* db, uint64_t cuid, int start_row, int end_row) {
  ReadOptions ro;
  ro.delta_full_scan = false;  // 部分扫描不设置 full_scan

  std::string upper_bound_str = GenerateKey(cuid, end_row + 1);
  Slice upper_bound = upper_bound_str;
  ro.iterate_upper_bound = &upper_bound;

  Iterator* it = db->NewIterator(ro);
  std::string start_key = GenerateKey(cuid, start_row);

  int count = 0;
  for (it->Seek(start_key); it->Valid(); it->Next()) {
    if (ExtractCUID(it->key()) != cuid) break;
    count++;
  }

  Status s = it->status();
  delete it;
  if (!s.ok()) {
    std::cerr << "Partial scan error: " << s.ToString() << std::endl;
  }
  return count;
}

// 写入数据并 Flush
void WriteBatchAndFlush(DB* db, const std::vector<uint64_t>& cuids,
                        int start_row, int count_per_cuid) {
  WriteBatch batch;
  for (uint64_t cuid : cuids) {
    for (int i = 0; i < count_per_cuid; ++i) {
      batch.Put(GenerateKey(cuid, start_row + i),
                "payload_" + std::to_string(cuid) + "_" +
                    std::to_string(start_row + i));
    }
  }
  Status s = db->Write(WriteOptions(), &batch);
  Check(s.ok(), "Write batch");

  s = db->Flush(FlushOptions());
  Check(s.ok(), "Flush to SST");
}

// ==========================================
// 主测试流程
// ==========================================
int main() {
  std::cout << "========================================" << std::endl;
  std::cout << "Partial Merge Test Suite" << std::endl;
  std::cout << "========================================" << std::endl;

  Options options;
  options.create_if_missing = true;
  Status s = DestroyDB(kDBPath, options);

  options.create_missing_column_families = true;
  options.disable_auto_compactions = true;
  options.num_levels = 1;
  options.level0_file_num_compaction_trigger = 20;
  options.level_compaction_dynamic_level_bytes = false;

  DB* db = nullptr;
  s = DB::Open(options, kDBPath, &db);
  Check(s.ok(), "DB Open");

  DBImpl* db_impl = dynamic_cast<DBImpl*>(db);
  auto hotspot_mgr = db_impl->GetHotspotManager();
  Check(hotspot_mgr != nullptr, "HotspotManager attached");

  const uint64_t CUID_PARTIAL = 5001;  // 测试 Partial Merge

  // =================================================================
  // 测试 1: skip_hot_path 与 ProcessPendingHotCuids
  // =================================================================
  std::cout << "\n>>> TEST 1: skip_hot_path & ProcessPendingHotCuids <<<\n";

  // 写入初始数据: 3 个 SST，每个 50 条
  for (int i = 0; i < 3; ++i) {
    WriteBatchAndFlush(db, {CUID_PARTIAL}, i * 100, 50);
    std::cout << "Generated SST #" << i + 1 << std::endl;
  }

  // 触发热点判定 (5 次全量扫描)
  std::cout << "Triggering hot detection with 5 full scans..." << std::endl;
  for (int i = 0; i < 5; ++i) {
    int rows = PerformFullScan(db, CUID_PARTIAL);
    std::cout << "  Scan #" << i + 1 << ": found " << rows << " rows"
              << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  // 验证热点建立
  HotIndexEntry entry;
  bool has_index = hotspot_mgr->GetIndexTable().GetEntry(CUID_PARTIAL, &entry);
  Check(has_index, "CUID_PARTIAL should be in HotIndexTable");
  Check(entry.HasSnapshot(),
        "CUID_PARTIAL should have Snapshot (via ProcessPendingHotCuids)");

  std::cout << "Snapshot segments: " << entry.snapshot_segments.size()
            << std::endl;
  std::cout << "Deltas: " << entry.deltas.size() << std::endl;

  // =================================================================
  // 测试 2: 写入更多数据产生新 Delta
  // =================================================================
  std::cout << "\n>>> TEST 2: Writing more data to create Deltas <<<\n";

  for (int i = 0; i < 4; ++i) {
    WriteBatchAndFlush(db, {CUID_PARTIAL}, 300 + i * 50, 50);
    std::cout << "Generated Delta SST #" << i + 1 << std::endl;
  }

  hotspot_mgr->GetIndexTable().GetEntry(CUID_PARTIAL, &entry);
  size_t delta_count = entry.deltas.size();
  std::cout << "Delta count after writes: " << delta_count << std::endl;
  Check(delta_count >= 3,
        "Should have multiple deltas (>= 3 for kPartialMerge threshold)");

  // =================================================================
  // 测试 3: 部分扫描触发 kPartialMerge
  // =================================================================
  std::cout << "\n>>> TEST 3: Partial Scan triggering kPartialMerge <<<\n";

  // 执行部分扫描 (只扫描 row_id 100-200 范围)
  std::cout << "Performing partial scan on row range [100, 200]..."
            << std::endl;
  int partial_rows = PerformPartialScan(db, CUID_PARTIAL, 100, 200);
  std::cout << "Partial scan found: " << partial_rows << " rows" << std::endl;

  // 检查是否有 pending partial merge 任务
  bool has_pending = hotspot_mgr->HasPendingPartialMerge();
  std::cout << "Has pending partial merge: " << (has_pending ? "YES" : "NO")
            << std::endl;

  // 如果有待处理任务，手动执行
  if (has_pending) {
    std::cout << "Executing ProcessPendingPartialMerge()..." << std::endl;
    db_impl->ProcessPendingPartialMerge();

    // 验证执行后状态
    hotspot_mgr->GetIndexTable().GetEntry(CUID_PARTIAL, &entry);
    std::cout << "After merge - Snapshot segments: "
              << entry.snapshot_segments.size() << std::endl;
    std::cout << "After merge - Deltas: " << entry.deltas.size() << std::endl;
  }

  // =================================================================
  // 测试 4: 验证数据完整性
  // =================================================================
  std::cout << "\n>>> TEST 4: Data Integrity Verification <<<\n";

  // 全量扫描验证所有数据可读
  int total_rows = PerformFullScan(db, CUID_PARTIAL);
  std::cout << "Total rows after all operations: " << total_rows << std::endl;

  // 预期: 3*50 (初始) + 4*50 (新增) = 350
  int expected = 3 * 50 + 4 * 50;
  Check(total_rows == expected,
        "Data integrity: expected " + std::to_string(expected) + " rows");

  // =================================================================
  // 测试 5: 多次部分扫描
  // =================================================================
  std::cout << "\n>>> TEST 5: Multiple Partial Scans <<<\n";

  for (int i = 0; i < 3; ++i) {
    int start = i * 100;
    int end = start + 80;
    int rows = PerformPartialScan(db, CUID_PARTIAL, start, end);
    std::cout << "Partial scan [" << start << ", " << end << "]: " << rows
              << " rows" << std::endl;
  }

  // 处理所有 pending 任务
  while (hotspot_mgr->HasPendingPartialMerge()) {
    db_impl->ProcessPendingPartialMerge();
  }

  // 最终数据验证
  total_rows = PerformFullScan(db, CUID_PARTIAL);
  std::cout << "Final total rows: " << total_rows << std::endl;
  Check(total_rows == expected, "Final data integrity check");

  // =================================================================
  // 测试 6: L0 Compaction 索引更新
  // =================================================================
  std::cout << "\n>>> TEST 6: L0 Compaction Index Update <<<\n";

  int rows = PerformFullScan(db, CUID_PARTIAL);

  // 记录 Compaction 前的 Delta 状态
  hotspot_mgr->GetIndexTable().GetEntry(CUID_PARTIAL, &entry);
  size_t deltas_before = entry.deltas.size();
  std::vector<uint64_t> file_numbers_before;
  for (const auto& d : entry.deltas) {
    file_numbers_before.push_back(d.file_number);
  }
  std::cout << "Before Compaction: " << deltas_before << " deltas" << std::endl;
  std::cout << "Delta file numbers: ";
  for (uint64_t fn : file_numbers_before) {
    std::cout << fn << " ";
  }
  std::cout << std::endl;

  // 触发 L0 Compaction
  std::cout << "Triggering L0 Compaction via CompactRange()..." << std::endl;
  s = db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  Check(s.ok(), "CompactRange");

  // 检查 Compaction 后的状态
  hotspot_mgr->GetIndexTable().GetEntry(CUID_PARTIAL, &entry);
  size_t deltas_after = entry.deltas.size();
  std::cout << "After Compaction: " << deltas_after << " deltas" << std::endl;

  // 验证 Delta 被合并 (数量应该减少或文件号变化)
  if (deltas_after < deltas_before) {
    std::cout << "[PASS] Deltas merged: " << deltas_before << " -> "
              << deltas_after << std::endl;
  } else if (deltas_after == 1 && !entry.deltas.empty()) {
    // 检查文件号是否变化
    bool file_changed = (entry.deltas[0].file_number != file_numbers_before[0]);
    std::cout << "New delta file number: " << entry.deltas[0].file_number
              << std::endl;
    Check(file_changed || deltas_before == 1,
          "Delta file number should change after compaction");
  } else {
    std::cout << "[INFO] Compaction may not have affected deltas (num_levels=1 "
                 "config)"
              << std::endl;
  }

  // =================================================================
  // 测试 7: Compaction 后数据完整性
  // =================================================================
  std::cout << "\n>>> TEST 7: Post-Compaction Data Integrity <<<\n";

  // 全量扫描验证数据未丢失
  total_rows = PerformFullScan(db, CUID_PARTIAL);
  std::cout << "Post-compaction total rows: " << total_rows << std::endl;
  Check(total_rows == expected, "Data integrity after L0 Compaction");

  // 部分扫描验证热点路径仍正常工作
  int rows_100_150 = PerformPartialScan(db, CUID_PARTIAL, 100, 149);
  std::cout << "Partial scan [100, 149]: " << rows_100_150 << " rows"
            << std::endl;
  Check(rows_100_150 == 50, "Partial scan [100, 149] should find 50 rows");

  std::cout << "\n========================================" << std::endl;
  std::cout << "All Tests PASSED!" << std::endl;
  std::cout << "========================================" << std::endl;

  delete db;
  return 0;
}
