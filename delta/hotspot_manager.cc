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
  // delete_table_.IncrementRefCount(cuid);

  if (!is_hot) {
    // no scan-as-compaction
    return;
  }

  // 尝试追加到 buffer
  // Append 内部已经加锁  
  bool needs_flush = buffer_.Append(cuid, key, value);

  if (needs_flush) {
    // TODO：目前实现，直接在当前线程 flush，后续需要改为后台线程？
    Status s = FlushGlobalBufferToSST();
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

Status HotspotManager::FlushGlobalBufferToSST() {
  std::vector<HotEntry> entries = buffer_.ExtractAndReset();
  if (entries.empty()) return Status::OK();

  // TODO: 生产环境应使用 ColumnFamily 的 Comparator 
  std::sort(entries.begin(), entries.end(), [](const HotEntry& a, const HotEntry& b) {
    if (a.cuid != b.cuid) {
      return a.cuid < b.cuid;
    }
    return a.key < b.key;
  });

  EnvOptions env_options;
  SstFileWriter sst_writer(env_options, db_options_);
  
  // filename: hot_shared_{timestamp}.sst
  auto now = std::chrono::system_clock::now();
  uint64_t file_number = std::chrono::duration_cast<std::chrono::microseconds>(
                             now.time_since_epoch()).count(); // timestamp
  std::string file_path = data_dir_ + "/hot_shared_" + std::to_string(file_number) + ".sst";

  Status s = sst_writer.Open(file_path);
  if (!s.ok()) return s;

  lifecycle_manager_->RegisterFile(file_number, file_path);

  // 写入数据并记录涉及的 CUID
  uint64_t current_cuid = 0;
  uint64_t start_offset = 0; 
  bool first_entry = true;

  start_offset = sst_writer.FileSize(); 

  for (size_t i = 0; i < entries.size(); ++i) {
    const auto& entry = entries[i];

    // CUID 切换
    if (!first_entry && entry.cuid != current_cuid) {
      // 结算上一个 CUID 的 Segment
      uint64_t current_size = sst_writer.FileSize();
      uint64_t length = current_size - start_offset;
      
      if (length > 0) {
        index_table_.AddSegment(current_cuid, {file_number, start_offset, length});
      }

      // 更新状态
      start_offset = current_size;
      current_cuid = entry.cuid;
    } 
  }

  if (!entries.empty()) {
    // Finish 之前需要计算最后一段的 Size? 
    // 注意：SstFileWriter 此时还没有写 Footer/IndexBlock。
    // 我们记录的 Length 是 "Data Block 的累计大小"，不包含文件 Footer。
    // 这对于 Read 来说是可以接受的，只要读取时知道大致范围。
    // 实际上，更精确的做法是 Read 时只依赖 FileID，Offset/Length 仅作为预读/IO优化的 hint。
    
    uint64_t current_size = sst_writer.FileSize();
    uint64_t length = current_size - start_offset;
    index_table_.AddSegment(current_cuid, {file_number, start_offset, length});
  }

  ExternalSstFileInfo file_info;
  s = sst_writer.Finish(&file_info);
  if (!s.ok()) return s;

  fprintf(stdout, "[HotspotManager] Generated Shared SST: %s (Num: %lu, Size: %lu)\n", 
          file_path.c_str(), file_number, file_info.file_size);

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE