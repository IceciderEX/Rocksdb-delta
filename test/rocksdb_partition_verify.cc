#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"

using namespace ROCKSDB_NAMESPACE;

namespace {

const std::string kDBPath = "/home/jx/Rocksdb-delta/db_tmp_partition_verify";
constexpr uint64_t kDbId = 1;
constexpr int kRowsPerTable = 5000;

void EncodeUint64BE(uint64_t v, char* out) {
  for (int i = 0; i < 8; ++i) {
    out[i] = static_cast<char>((v >> (56 - 8 * i)) & 0xFF);
  }
}

uint64_t DecodeUint64BE(const char* in) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v = (v << 8) | static_cast<uint64_t>(static_cast<unsigned char>(in[i]));
  }
  return v;
}

std::string MakeKey(uint64_t dbid, uint64_t tableid, uint64_t cuid,
                    uint64_t rowid) {
  std::string key;
  key.resize(32);
  EncodeUint64BE(dbid, &key[0]);
  EncodeUint64BE(tableid, &key[8]);
  EncodeUint64BE(cuid, &key[16]);
  EncodeUint64BE(rowid, &key[24]);
  return key;
}

uint64_t ExtractTableId(const Slice& key) {
  if (key.size() < 16) {
    return 0;
  }
  return DecodeUint64BE(key.data() + 8);
}

int32_t PartitionOf(uint64_t tableid) {
  return static_cast<int32_t>(tableid % 16);
}

bool WriteTableRows(DB* db, uint64_t tableid, int rows) {
  WriteBatch wb;
  const uint64_t cuid = tableid * 100;
  for (int i = 0; i < rows; ++i) {
    wb.Put(MakeKey(kDbId, tableid, cuid, static_cast<uint64_t>(i)), "value");
  }
  Status s = db->Write(WriteOptions(), &wb);
  if (!s.ok()) {
    std::cerr << "Write failed, table=" << tableid << ", status="
              << s.ToString() << std::endl;
    return false;
  }
  return true;
}

bool ScanAndCheck(DB* db, const ReadOptions& ro, int expected_count,
                  bool strict_partition_check, int32_t expected_partition) {
  std::string start_key = MakeKey(kDbId, 0, 0, 0);
  std::string upper_key = MakeKey(kDbId + 1, 0, 0, 0);

  ReadOptions read_options = ro;
  Slice upper(upper_key);
  read_options.iterate_upper_bound = &upper;

  std::unique_ptr<Iterator> it(db->NewIterator(read_options));
  int count = 0;
  for (it->Seek(start_key); it->Valid(); it->Next()) {
    if (strict_partition_check) {
      uint64_t tableid = ExtractTableId(it->key());
      int32_t pid = PartitionOf(tableid);
      if (pid != expected_partition) {
        std::cerr << "Partition mismatch in scan result: tableid=" << tableid
                  << ", partition=" << pid
                  << ", expected_partition=" << expected_partition
                  << std::endl;
        return false;
      }
    }
    ++count;
  }

  if (!it->status().ok()) {
    std::cerr << "Iterator failed: " << it->status().ToString() << std::endl;
    return false;
  }

  if (count != expected_count) {
    std::cerr << "Count mismatch. expected=" << expected_count
              << ", actual=" << count << std::endl;
    return false;
  }
  return true;
}

bool MultiGetAndCheck(DB* db, const ReadOptions& ro,
                      const std::vector<std::string>& keys,
                      const std::vector<bool>& expected_found) {
  if (keys.size() != expected_found.size()) {
    std::cerr << "Invalid test input: keys/expected_found size mismatch"
              << std::endl;
    return false;
  }

  std::vector<Slice> key_slices;
  key_slices.reserve(keys.size());
  for (const auto& key : keys) {
    key_slices.emplace_back(key);
  }

  std::vector<std::string> values;
  std::vector<Status> statuses = db->MultiGet(ro, key_slices, &values);
  if (statuses.size() != expected_found.size()) {
    std::cerr << "Unexpected MultiGet status count. expected="
              << expected_found.size() << ", actual=" << statuses.size()
              << std::endl;
    return false;
  }

  for (size_t i = 0; i < statuses.size(); ++i) {
    const bool found = statuses[i].ok();
    if (found != expected_found[i]) {
      std::cerr << "MultiGet mismatch at index " << i << ", expected_found="
                << expected_found[i] << ", actual_status="
                << statuses[i].ToString() << std::endl;
      return false;
    }
  }

  return true;
}

bool RunPartitionVerify() {
  Options options;
  options.create_if_missing = true;
  options.enable_delta = true;
  options.delta_options.enable_partition = true;
  options.num_levels = 2;

  DestroyDB(kDBPath, options);

  DB* db = nullptr;
  Status s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) {
    std::cerr << "Open failed: " << s.ToString() << std::endl;
    return false;
  }

  // tableid 1/17 -> partition 1, 2/18 -> partition 2, 3 -> partition 3
  if (!WriteTableRows(db, 1, kRowsPerTable) ||
      !WriteTableRows(db, 17, kRowsPerTable) ||
      !WriteTableRows(db, 2, kRowsPerTable) ||
      !WriteTableRows(db, 18, kRowsPerTable) ||
      !WriteTableRows(db, 3, kRowsPerTable)) {
    delete db;
    return false;
  }

  s = db->Flush(FlushOptions());
  if (!s.ok()) {
    std::cerr << "Flush failed: " << s.ToString() << std::endl;
    delete db;
    return false;
  }

  const int total_rows = kRowsPerTable * 5;
  const int partition1_rows = kRowsPerTable * 2;
  const int partition2_rows = kRowsPerTable * 2;
  const int partition3_rows = kRowsPerTable;

  ReadOptions ro_all;
  if (!ScanAndCheck(db, ro_all, total_rows, false, -1)) {
    delete db;
    return false;
  }

  ReadOptions ro_p1;
  ro_p1.read_partition_id = 1;
  if (!ScanAndCheck(db, ro_p1, partition1_rows, true, 1)) {
    delete db;
    return false;
  }

  ReadOptions ro_p2;
  ro_p2.read_partition_id = 2;
  if (!ScanAndCheck(db, ro_p2, partition2_rows, true, 2)) {
    delete db;
    return false;
  }

  ReadOptions ro_p3;
  ro_p3.read_partition_id = 3;
  if (!ScanAndCheck(db, ro_p3, partition3_rows, true, 3)) {
    delete db;
    return false;
  }

  ReadOptions ro_p9;
  ro_p9.read_partition_id = 9;
  if (!ScanAndCheck(db, ro_p9, 0, true, 9)) {
    delete db;
    return false;
  }

  const std::string key_p1 = MakeKey(kDbId, 1, 100, 0);
  const std::string key_p2 = MakeKey(kDbId, 2, 200, 0);
  const std::vector<std::string> check_keys = {key_p1, key_p2};

  if (!MultiGetAndCheck(db, ro_all, check_keys, {true, true})) {
    delete db;
    return false;
  }
  if (!MultiGetAndCheck(db, ro_p1, check_keys, {true, false})) {
    delete db;
    return false;
  }
  if (!MultiGetAndCheck(db, ro_p2, check_keys, {false, true})) {
    delete db;
    return false;
  }

  delete db;

  // Reopen: verify partition metadata persisted to MANIFEST and still works.
  s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) {
    std::cerr << "Reopen failed: " << s.ToString() << std::endl;
    return false;
  }
  if (!ScanAndCheck(db, ro_p1, partition1_rows, true, 1)) {
    delete db;
    return false;
  }
  delete db;

  // Disable partition feature: read_partition_id should be ignored.
  options.create_if_missing = false;
  options.delta_options.enable_partition = false;
  s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) {
    std::cerr << "Open(disabled partition) failed: " << s.ToString() << std::endl;
    return false;
  }

  if (!ScanAndCheck(db, ro_p1, total_rows, false, -1)) {
    delete db;
    return false;
  }

  delete db;

  // Recreate DB and verify partition reads still work when flush falls back
  // (e.g., memtable contains range tombstones).
  options.create_if_missing = true;
  options.delta_options.enable_partition = true;
  DestroyDB(kDBPath, options);

  s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) {
    std::cerr << "Open(range-delete fallback) failed: " << s.ToString()
              << std::endl;
    return false;
  }

  constexpr int kFallbackRows = 1000;
  if (!WriteTableRows(db, 1, kFallbackRows) ||
      !WriteTableRows(db, 2, kFallbackRows)) {
    delete db;
    return false;
  }

  WriteBatch wb;
  wb.DeleteRange(MakeKey(kDbId + 10, 0, 0, 0), MakeKey(kDbId + 10, 0, 0, 1));
  s = db->Write(WriteOptions(), &wb);
  if (!s.ok()) {
    std::cerr << "Write(range tombstone) failed: " << s.ToString()
              << std::endl;
    delete db;
    return false;
  }

  s = db->Flush(FlushOptions());
  if (!s.ok()) {
    std::cerr << "Flush(range-delete fallback) failed: " << s.ToString()
              << std::endl;
    delete db;
    return false;
  }

  ReadOptions ro_fallback_p1;
  ro_fallback_p1.read_partition_id = 1;
  if (!ScanAndCheck(db, ro_fallback_p1, kFallbackRows, true, 1)) {
    delete db;
    return false;
  }

  ReadOptions ro_fallback_p2;
  ro_fallback_p2.read_partition_id = 2;
  if (!ScanAndCheck(db, ro_fallback_p2, kFallbackRows, true, 2)) {
    delete db;
    return false;
  }

  const std::string fallback_key_p1 = MakeKey(kDbId, 1, 100, 0);
  const std::string fallback_key_p2 = MakeKey(kDbId, 2, 200, 0);
  const std::vector<std::string> fallback_keys = {fallback_key_p1,
                                                  fallback_key_p2};
  if (!MultiGetAndCheck(db, ro_fallback_p1, fallback_keys, {true, false})) {
    delete db;
    return false;
  }
  if (!MultiGetAndCheck(db, ro_fallback_p2, fallback_keys, {false, true})) {
    delete db;
    return false;
  }

  delete db;
  std::cout << "Partition verify test PASSED." << std::endl;
  return true;
}

}  // namespace

int main() {
  bool ok = RunPartitionVerify();
  if (!ok) {
    std::cerr << "Partition verify test FAILED." << std::endl;
    return 1;
  }
  return 0;
}
