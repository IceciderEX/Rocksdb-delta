//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "delta/hotspot_manager.h"

#include <chrono>
#include <sstream>
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/env.h"
#include "port/port.h"

namespace ROCKSDB_NAMESPACE {

HotspotManager::HotspotManager(const Options& db_options, const std::string& data_dir)
    : db_options_(db_options), 
      data_dir_(data_dir),
      frequency_table_(4, 600) { 
  db_options_.env->CreateDirIfMissing(data_dir_);
}

uint64_t HotspotManager::ExtractCUID(const Slice& key) {
  // TODO: 根据 Key Schema 实现 CUID 解析逻辑
  if (key.size() < 24) {
    return 0; 
  }

  const unsigned char* p = reinterpret_cast<const unsigned char*>(key.data()) + 16;

  // Big-Endian Decoding
  uint64_t cuid = (static_cast<uint64_t>(p[0]) << 56) |
                  (static_cast<uint64_t>(p[1]) << 48) |
                  (static_cast<uint64_t>(p[2]) << 40) |
                  (static_cast<uint64_t>(p[3]) << 32) |
                  (static_cast<uint64_t>(p[4]) << 24) |
                  (static_cast<uint64_t>(p[5]) << 16) |
                  (static_cast<uint64_t>(p[6]) << 8)  |
                  (static_cast<uint64_t>(p[7]));

  return cuid;
}

void HotspotManager::OnUserScan(const Slice& key, const Slice& value) {
  uint64_t cuid = ExtractCUID(key);
  if (cuid == 0) return; // 解析失败或非目标 Key

  // hot cuid check
  bool is_hot = frequency_table_.RecordAndCheckHot(cuid);

  delete_table_.IncrementRefCount(cuid);

  if (!is_hot) {
    // no scan-as-compaction
    return;
  }

  // 尝试追加到 buffer
  // Append 内部已经加锁  
  bool needs_flush = buffer_.Append(cuid, key, value);

  if (needs_flush) {
    // TODO：目前实现，直接在当前线程 flush，后续需要改为后台线程？
    Status s = FlushBufferToSST(cuid);
    if (!s.ok()) {
      fprintf(stderr, "[HotspotManager] Flush SST failed for CUID %lu: %s\n", 
              cuid, s.ToString().c_str());
    }
  }
}

bool HotspotManager::InterceptDelete(const Slice& key) {
  uint64_t cuid = ExtractCUID(key);
  if (cuid == 0) return false;

  // 在 GDCT 中查询是否该 cuid 被标记为删除
  bool marked = delete_table_.MarkDeleted(cuid);
  
  if (marked) {
    // fprintf(stderr, "[HotspotManager] Intercepted Delete for CUID: %lu\n", cuid);
    return true;
  }

  // CUID 不在热点管理范围内
  return false;
}

std::string HotspotManager::GenerateSstFileName(uint64_t cuid) {
  auto now = std::chrono::system_clock::now();
  auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                       now.time_since_epoch())
                       .count();
  
  std::stringstream ss;
  ss << data_dir_ << "/hot_" << cuid << "_" << timestamp << ".sst";
  return ss.str();
}

Status HotspotManager::FlushBufferToSST(uint64_t cuid) {
  HotBufferBatch batch = buffer_.ExtractBatch(cuid);
  if (batch.IsEmpty()) {
    return Status::OK();
  }

  EnvOptions env_options;
  SstFileWriter sst_writer(env_options, db_options_);

  std::string file_path = GenerateSstFileName(cuid);

  Status s = sst_writer.Open(file_path);
  if (!s.ok()) {
    return s;
  }

  for (size_t i = 0; i < batch.keys.size(); ++i) {
    s = sst_writer.Put(batch.keys[i], batch.values[i]);
    if (!s.ok()) {
      return s;
    }
  }

  ExternalSstFileInfo file_info;
  s = sst_writer.Finish(&file_info);
  if (!s.ok()) {
    return s;
  }

  index_table_.UpdateSnapshot(cuid, file_path);

  // log
  fprintf(stdout, "[HotspotManager] Generated SST: %s (Size: %lu bytes, Rows: %lu)\n", 
          file_path.c_str(), file_info.file_size, file_info.num_entries);

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE