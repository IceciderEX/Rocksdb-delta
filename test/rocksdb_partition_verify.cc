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
