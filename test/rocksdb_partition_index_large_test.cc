#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"

using namespace ROCKSDB_NAMESPACE;

namespace {

const std::string kDBPath = "/home/jx/Rocksdb-delta/db_tmp_partition_index_large";
constexpr uint64_t kDbId = 1;
constexpr int kPartitionCount = 16;
constexpr int kTargetPartition = 3;
constexpr int kTableCount = 256;
constexpr int kRowsPerTable = 2000;
constexpr int kWriteBatchRows = 256;

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
  return static_cast<int32_t>(tableid % kPartitionCount);
}

struct ScanResult {
  bool ok = false;
  int64_t row_count = 0;
  int64_t elapsed_ms = 0;
};

bool WriteLargeDataset(DB* db) {
  for (int table_idx = 0; table_idx < kTableCount; ++table_idx) {
    const uint64_t tableid = static_cast<uint64_t>(table_idx + 1);
    const uint64_t cuid = tableid * 100;

    for (int start = 0; start < kRowsPerTable; start += kWriteBatchRows) {
      const int end = std::min(kRowsPerTable, start + kWriteBatchRows);
      WriteBatch wb;
      for (int row = start; row < end; ++row) {
        wb.Put(MakeKey(kDbId, tableid, cuid, static_cast<uint64_t>(row)),
               "value");
      }
      Status s = db->Write(WriteOptions(), &wb);
      if (!s.ok()) {
        std::cerr << "Write failed, table=" << tableid << ", rows=[" << start
                  << "," << end << "), status=" << s.ToString()
                  << std::endl;
        return false;
      }
    }

    // Flush per table to keep many separate L0 files, making index effect visible.
    Status fs = db->Flush(FlushOptions());
    if (!fs.ok()) {
      std::cerr << "Flush failed after table=" << tableid
                << ", status=" << fs.ToString() << std::endl;
      return false;
    }

    if ((table_idx + 1) % 32 == 0) {
      std::cout << "Written tables: " << (table_idx + 1) << "/" << kTableCount
                << std::endl;
    }
  }
  return true;
}

ScanResult ScanRange(DB* db, const ReadOptions& base_ro,
                     bool strict_partition_check, int32_t expected_partition) {
  ScanResult result;
  std::string start_key = MakeKey(kDbId, 0, 0, 0);
  std::string upper_key = MakeKey(kDbId + 1, 0, 0, 0);

  ReadOptions ro = base_ro;
  Slice upper(upper_key);
  ro.iterate_upper_bound = &upper;

  auto t0 = std::chrono::steady_clock::now();
  std::unique_ptr<Iterator> it(db->NewIterator(ro));
  int64_t count = 0;

  for (it->Seek(start_key); it->Valid(); it->Next()) {
    if (strict_partition_check) {
      const uint64_t tableid = ExtractTableId(it->key());
      const int32_t pid = PartitionOf(tableid);
      if (pid != expected_partition) {
        std::cerr << "Partition mismatch in scan result: tableid=" << tableid
                  << ", partition=" << pid
                  << ", expected_partition=" << expected_partition
                  << std::endl;
        return result;
      }
    }
    ++count;
  }

  auto t1 = std::chrono::steady_clock::now();
  result.elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

  if (!it->status().ok()) {
    std::cerr << "Iterator failed: " << it->status().ToString() << std::endl;
    return result;
  }

  result.ok = true;
  result.row_count = count;
  return result;
}

bool CheckPartitionIndexFiles(DB* db) {
  DBImpl* db_impl = static_cast<DBImpl*>(db);
  SuperVersion* sv = db_impl->GetAndRefSuperVersion(static_cast<uint32_t>(0));
  if (sv == nullptr) {
    std::cerr << "GetAndRefSuperVersion failed" << std::endl;
    return false;
  }

  const auto* vstorage = sv->current->storage_info();
  const auto& l0_all = vstorage->LevelFiles(0);
  const auto& l0_partition =
      vstorage->Level0FilesForPartition(kTargetPartition);

  const size_t expected_tables_in_partition =
      static_cast<size_t>(kTableCount / kPartitionCount);

  std::cout << "L0 files total=" << l0_all.size()
            << ", target_partition_files=" << l0_partition.size()
            << ", expected_tables_in_partition=" << expected_tables_in_partition
            << std::endl;

  bool ok = true;
  if (l0_all.empty()) {
    std::cerr << "No L0 files found; cannot validate partition index" << std::endl;
    ok = false;
  }
  if (l0_partition.empty()) {
    std::cerr << "Partition index returned empty candidate files" << std::endl;
    ok = false;
  }
  if (!l0_all.empty() && l0_partition.size() >= l0_all.size()) {
    std::cerr << "Partition index ineffective: candidate files are not reduced"
              << std::endl;
    ok = false;
  }
  if (l0_partition.size() > expected_tables_in_partition + 2) {
    std::cerr << "Partition index candidate file count is unexpectedly high"
              << std::endl;
    ok = false;
  }

  db_impl->ReturnAndCleanupSuperVersion(static_cast<uint32_t>(0), sv);
  return ok;
}

bool RunLargePartitionIndexTest() {
  Options options;
  options.create_if_missing = true;
  options.enable_delta = true;
  options.delta_options.enable_partition = true;
  options.disable_auto_compactions = true;
  options.num_levels = 2;
  options.write_buffer_size = 1 << 20;

  DestroyDB(kDBPath, options);

  DB* db = nullptr;
  Status s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) {
    std::cerr << "Open failed: " << s.ToString() << std::endl;
    return false;
  }

  if (!WriteLargeDataset(db)) {
    delete db;
    return false;
  }

  if (!CheckPartitionIndexFiles(db)) {
    delete db;
    return false;
  }

  const int64_t expected_all_rows =
      static_cast<int64_t>(kTableCount) * kRowsPerTable;
  const int64_t expected_partition_rows =
      static_cast<int64_t>(kTableCount / kPartitionCount) * kRowsPerTable;

  ReadOptions ro_all;
  ScanResult all_scan = ScanRange(db, ro_all, false, -1);
  if (!all_scan.ok || all_scan.row_count != expected_all_rows) {
    std::cerr << "All-scan mismatch, expected=" << expected_all_rows
              << ", actual=" << all_scan.row_count << std::endl;
    delete db;
    return false;
  }

  ReadOptions ro_partition;
  ro_partition.read_partition_id = kTargetPartition;
  ScanResult partition_scan =
      ScanRange(db, ro_partition, true, kTargetPartition);
  if (!partition_scan.ok || partition_scan.row_count != expected_partition_rows) {
    std::cerr << "Partition-scan mismatch, expected=" << expected_partition_rows
              << ", actual=" << partition_scan.row_count << std::endl;
    delete db;
    return false;
  }

  std::cout << "All-scan rows=" << all_scan.row_count
            << ", elapsed_ms=" << all_scan.elapsed_ms << std::endl;
  std::cout << "Partition-scan rows=" << partition_scan.row_count
            << ", elapsed_ms=" << partition_scan.elapsed_ms << std::endl;

  if (partition_scan.elapsed_ms > all_scan.elapsed_ms) {
    std::cout << "Note: partition scan not faster in this run, but correctness "
                 "and candidate-file reduction are verified."
              << std::endl;
  }

  delete db;
  std::cout << "Large partition/index test PASSED." << std::endl;
  return true;
}

}  // namespace

int main() {
  bool ok = RunLargePartitionIndexTest();
  if (!ok) {
    std::cerr << "Large partition/index test FAILED." << std::endl;
    return 1;
  }
  return 0;
}
