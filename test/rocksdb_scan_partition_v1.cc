#include <cstdint>
#include <cstring>
#include <atomic>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"

using namespace ROCKSDB_NAMESPACE;

namespace {

const std::string kDBPath = "/home/jx/Rocksdb-delta/db_tmp";
constexpr uint64_t kDbId = 1;
constexpr int kNumWriterThreads = 16;
constexpr int kBatchSize = 128;
constexpr int kTargetPutBatches = 200;   // 与 rocksdb_scan_v1 同量级
constexpr int kRowsPerWriter = kBatchSize * kTargetPutBatches;  // 25600

inline void EncodeUint64BE(uint64_t v, char* out) {
  for (int i = 0; i < 8; ++i) {
    out[i] = static_cast<char>((v >> (56 - 8 * i)) & 0xFF);
  }
}

std::string MakeKey(uint64_t dbid, uint64_t tableid, uint64_t cuid, uint64_t rowid) {
  std::string key;
  key.resize(32);
  EncodeUint64BE(dbid, &key[0]);
  EncodeUint64BE(tableid, &key[8]);
  EncodeUint64BE(cuid, &key[16]);
  EncodeUint64BE(rowid, &key[24]);
  return key;
}

uint64_t DecodeTableId(const Slice& key) {
  if (key.size() < 16) {
    return 0;
  }
  uint64_t tableid = 0;
  const unsigned char* p = reinterpret_cast<const unsigned char*>(key.data()) + 8;
  for (int i = 0; i < 8; ++i) {
    tableid = (tableid << 8) | p[i];
  }
  return tableid;
}

int32_t PartitionOf(uint64_t tableid) {
  return static_cast<int32_t>(tableid % 16);
}

uint64_t TableIdForWriter(int writer_id) {
  // 8 个线程落到 partition=1，4 个线程落到 partition=2，4 个线程落到 partition=3。
  if (writer_id < 8) {
    return (writer_id % 2 == 0) ? 1 : 17;
  }
  if (writer_id < 12) {
    return (writer_id % 2 == 0) ? 2 : 18;
  }
  return (writer_id % 2 == 0) ? 3 : 19;
}

bool ConcurrentWrite(DB* db) {
  std::atomic<bool> ok(true);
  std::vector<std::thread> writers;
  writers.reserve(kNumWriterThreads);

  for (int i = 0; i < kNumWriterThreads; ++i) {
    writers.emplace_back([db, i, &ok]() {
      const uint64_t tableid = TableIdForWriter(i);
      // 让每个线程的 cuid 唯一，避免 key 冲突。
      const uint64_t cuid = tableid * 1000 + static_cast<uint64_t>(i);
      int row = 0;
      for (int b = 0; b < kTargetPutBatches; ++b) {
        WriteBatch wb;
        for (int k = 0; k < kBatchSize; ++k) {
          wb.Put(MakeKey(kDbId, tableid, cuid, static_cast<uint64_t>(row++)), "v");
        }
        const Status s = db->Write(WriteOptions(), &wb);
        if (!s.ok()) {
          std::cerr << "Writer " << i << " failed: " << s.ToString() << std::endl;
          ok.store(false);
          return;
        }
      }
    });
  }

  for (auto& t : writers) {
    t.join();
  }
  return ok.load();
}

bool ScanAndCheck(DB* db, const ReadOptions& ro, int expected_count,
                  int32_t expected_partition, bool enforce_partition_check) {
  const std::string start_key = MakeKey(kDbId, 0, 0, 0);
  const std::string end_key = MakeKey(kDbId + 1, 0, 0, 0);

  ReadOptions ro_copy = ro;
  Slice ub(end_key);
  ro_copy.iterate_upper_bound = &ub;

  std::unique_ptr<Iterator> it(db->NewIterator(ro_copy));
  int count = 0;
  for (it->Seek(start_key); it->Valid(); it->Next()) {
    if (enforce_partition_check) {
      const uint64_t tableid = DecodeTableId(it->key());
      if (PartitionOf(tableid) != expected_partition) {
        std::cerr << "Found mismatched key tableid=" << tableid
                  << " partition=" << PartitionOf(tableid)
                  << " expected partition=" << expected_partition << std::endl;
        return false;
      }
    }
    ++count;
  }

  if (!it->status().ok()) {
    std::cerr << "Iterator error: " << it->status().ToString() << std::endl;
    return false;
  }

  if (count != expected_count) {
    std::cerr << "Unexpected row count. expected=" << expected_count
              << " actual=" << count << std::endl;
    return false;
  }
  return true;
}

bool RepeatScanAndCheck(DB* db, const ReadOptions& ro, int expected_count,
                        int32_t expected_partition,
                        bool enforce_partition_check, int rounds,
                        const std::string& label) {
  for (int i = 0; i < rounds; ++i) {
    if (!ScanAndCheck(db, ro, expected_count, expected_partition,
                      enforce_partition_check)) {
      std::cerr << "Scan round failed, label=" << label
                << " round=" << (i + 1) << "/" << rounds << std::endl;
      return false;
    }
    if ((i + 1) % 100 == 0 || i + 1 == rounds) {
      std::cout << "[Progress] " << label << " " << (i + 1) << "/" << rounds
                << std::endl;
    }
  }
  return true;
}

bool ConcurrentScanAndCheck(DB* db, const ReadOptions& ro, int expected_count,
                            int32_t expected_partition,
                            bool enforce_partition_check,
                            const std::vector<int>& rounds_per_thread,
                            const std::string& label) {
  std::atomic<bool> ok(true);
  std::vector<std::thread> workers;
  workers.reserve(rounds_per_thread.size());

  for (size_t i = 0; i < rounds_per_thread.size(); ++i) {
    workers.emplace_back([&, i]() {
      if (rounds_per_thread[i] <= 0) {
        return;
      }
      const std::string worker_label =
          label + "-worker" + std::to_string(static_cast<int>(i));
      if (!RepeatScanAndCheck(db, ro, expected_count, expected_partition,
                              enforce_partition_check, rounds_per_thread[i],
                              worker_label)) {
        ok.store(false);
      }
    });
  }

  for (auto& t : workers) {
    t.join();
  }
  return ok.load();
}

bool RunPartitionValidation() {
  Options options;
  options.create_if_missing = true;
  options.enable_delta = true;
  options.delta_options.enable_partition = true;
  options.num_levels = 2;
  options.level0_file_num_compaction_trigger = 4;

  DestroyDB(kDBPath, options);

  DB* db = nullptr;
  Status s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) {
    std::cerr << "Open failed: " << s.ToString() << std::endl;
    return false;
  }

  std::cout << "[Phase] Concurrent write: threads=" << kNumWriterThreads
            << ", rows per writer=" << kRowsPerWriter << std::endl;
  if (!ConcurrentWrite(db)) {
    delete db;
    return false;
  }

  s = db->Flush(FlushOptions());
  if (!s.ok()) {
    std::cerr << "Flush failed: " << s.ToString() << std::endl;
    delete db;
    return false;
  }

  const int total_rows = kRowsPerWriter * kNumWriterThreads;
  const int p1_rows = kRowsPerWriter * 8;
  const int p2_rows = kRowsPerWriter * 4;
  const int p3_rows = kRowsPerWriter * 4;

  ReadOptions all_ro;
  if (!ConcurrentScanAndCheck(db, all_ro, total_rows, -1, false,
                              {75, 75}, "all partitions scan")) {
    delete db;
    return false;
  }

  ReadOptions p1_ro;
  p1_ro.read_partition_id = 1;
  // 总计 1000 次扫描，按 4 个并发线程分摊。
  if (!ConcurrentScanAndCheck(db, p1_ro, p1_rows, 1, true,
                              {250, 250, 250, 250}, "partition=1 scan")) {
    delete db;
    return false;
  }

  ReadOptions p2_ro;
  p2_ro.read_partition_id = 2;
  if (!ConcurrentScanAndCheck(db, p2_ro, p2_rows, 2, true,
                              {75, 75}, "partition=2 scan")) {
    delete db;
    return false;
  }

  ReadOptions p3_ro;
  p3_ro.read_partition_id = 3;
  if (!ConcurrentScanAndCheck(db, p3_ro, p3_rows, 3, true,
                              {75, 75}, "partition=3 scan")) {
    delete db;
    return false;
  }

  delete db;

  // 重启后验证 MANIFEST 恢复的 partition_id 仍然可用于读过滤。
  s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) {
    std::cerr << "Reopen failed: " << s.ToString() << std::endl;
    return false;
  }

  if (!ConcurrentScanAndCheck(db, p1_ro, p1_rows, 1, true,
                              {75, 75}, "reopen partition=1 scan")) {
    delete db;
    return false;
  }
  delete db;

  // 关闭分区开关后，read_partition_id 应被忽略，结果回到全量读取。
  options.create_if_missing = false;
  options.delta_options.enable_partition = false;
  s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) {
    std::cerr << "Open with partition disabled failed: " << s.ToString()
              << std::endl;
    return false;
  }

  if (!ConcurrentScanAndCheck(db, p1_ro, total_rows, 1, false,
                              {75, 75},
                              "partition disabled scan(ignore filter)")) {
    delete db;
    return false;
  }

  delete db;
  std::cout << "Partition validation passed." << std::endl;
  return true;
}

}  // namespace

int main() {
  const bool ok = RunPartitionValidation();
  if (!ok) {
    std::cerr << "Partition validation failed." << std::endl;
    return 1;
  }
  return 0;
}
