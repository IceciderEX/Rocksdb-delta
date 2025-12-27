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
    : db_options_(db_options), data_dir_(data_dir) {
  // 确保存储目录存在
  // 注意：实际生产环境可能需要更严谨的 Env::CreateDir 处理
  db_options_.env->CreateDirIfMissing(data_dir_);
}

uint64_t HotspotManager::ExtractCUID(const Slice& key) {
  // TODO: 【关键】请在此处根据你的 Key Schema 实现 CUID 解析逻辑
  // 你的 Key 格式: dbid_tableid_cuid_row_id_event_seq
  // 假设 dbid(8字节) + tableid(8字节) + cuid(8字节) ...
  
  if (key.size() < 24) {
    // 长度不足，可能不是数据 Key，或者 Schema 不对
    return 0; 
  }

  // 示例：假设 CUID 位于偏移量 16 处，是 64 位大端整数
  // const char* ptr = key.data() + 16;
  // uint64_t cuid = DecodeFixed64(ptr); 
  
  // 临时模拟：为了编译通过，假设前 8 字节通过某种哈希映射得到 CUID，或者直接硬编码
  // 实际代码请务必替换为真实的解析逻辑
  return 12345; // Placeholder
}

void HotspotManager::OnUserScan(const Slice& key, const Slice& value) {
  uint64_t cuid = ExtractCUID(key);
  if (cuid == 0) return; // 解析失败或非目标 Key

  // 尝试追加到 buffer
  // Append 内部已经加锁
  bool needs_flush = buffer_.Append(cuid, key, value);

  if (needs_flush) {
    // 触发刷盘
    // 注意：在第一阶段验证中，我们直接在当前线程同步执行 Flush。
    // 这会阻塞用户的 Scan 一小会儿，但在验证逻辑时是安全的。
    // 后续阶段（Task 3/4）我们会把这里改成异步后台线程执行。
    Status s = FlushBufferToSST(cuid);
    if (!s.ok()) {
      fprintf(stderr, "[HotspotManager] Flush SST failed for CUID %lu: %s\n", 
              cuid, s.ToString().c_str());
    }
  }
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
    s = sst_writer.Add(batch.keys[i], batch.values[i]);
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