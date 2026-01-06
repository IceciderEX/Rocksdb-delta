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
      lifecycle_manager_(std::make_shared<HotSstLifecycleManager>(db_options)),
      index_table_(lifecycle_manager_),
      frequency_table_(4, 600) { 
  db_options_.env->CreateDirIfMissing(data_dir_);
}

uint64_t HotspotManager::ExtractCUID(const Slice& key) {
  // TODO: 根据实际的 Key Schema提取 cuid，这里先假设一波
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

bool HotspotManager::RegisterScan(uint64_t cuid) {
  if (cuid == 0) return false;
  
  return frequency_table_.RecordAndCheckHot(cuid);
}

void HotspotManager::BufferHotData(uint64_t cuid, const Slice& key, const Slice& value) {
  bool needs_flush = buffer_.Append(cuid, key, value);

  if (needs_flush) {
    Status s = FlushGlobalBufferToSST();
    if (!s.ok()) {
      fprintf(stderr, "[HotspotManager] Flush SST failed for CUID %lu: %s\n", 
              cuid, s.ToString().c_str());
    }
  }
}

// void HotspotManager::OnUserScan(const Slice& key, const Slice& value) {
//   uint64_t cuid = ExtractCUID(key);
//   if (cuid == 0) return; // 解析失败或非目标 Key

//   if (delete_table_.IsDeleted(cuid)) {
//       return; 
//   }

//   // hot cuid check
//   bool is_hot = frequency_table_.RecordAndCheckHot(cuid);
//   // delete_table_.IncrementRefCount(cuid);

//   if (!is_hot) {
//     // no scan-as-compaction
//     return;
//   }

//   // 尝试追加到 buffer
//   // Append 内部已经加锁  
//   bool needs_flush = buffer_.Append(cuid, key, value);

//   if (needs_flush) {
//     // TODO：目前实现，直接在当前线程 flush，后续需要改为后台线程？
//     Status s = FlushGlobalBufferToSST();
//     if (!s.ok()) {
//       fprintf(stderr, "[HotspotManager] Flush SST failed for CUID %lu: %s\n", 
//               cuid, s.ToString().c_str());
//     }
//   }
// }

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


bool HotspotManager::ShouldTriggerScanAsCompaction(uint64_t cuid) {
  HotIndexEntry entry;
  if (!index_table_.GetEntry(cuid, &entry)) {
    // a)	当前热点CUid无Snapshot。
    return true;
  }
  // b)	已有Snapshot，且新增的Deltas片段数量超过5个。
  if (!entry.HasSnapshot() || entry.deltas.size() > 5) {
    return true;
  }
  return false;
}

class VectorIterator {
 public:
  VectorIterator(const std::vector<HotEntry>& data) 
      : data_(data), idx_(0) {}

  bool Valid() const { return idx_ < data_.size(); }
  void Next() { idx_++; }
  const Slice Key() const { return data_[idx_].key; }
  const Slice Value() const { return data_[idx_].value; }
  uint64_t Cuid() const { return data_[idx_].cuid; }

 private:
  const std::vector<HotEntry>& data_;
  size_t idx_;
};

void HotspotManager::FinalizeScanAsCompaction(uint64_t cuid) {
  std::vector<HotEntry> all_entries = buffer_.ExtractAndReset();
  std::vector<HotEntry> new_data;
  std::vector<HotEntry> remaining_entries; // 其他 CUID 的数据需要放回去?

  for (const auto& entry : all_entries) {
    if (entry.cuid == cuid) {
      new_data.push_back(entry);
    } else {
      // 非当前 CUID 数据？
    }
  }

  std::sort(new_data.begin(), new_data.end(), [](const HotEntry& a, const HotEntry& b) {
    return a.key < b.key;
  });
  HotIndexEntry index_entry;
  bool has_old_data = false;
  std::string old_file_path;

  if (index_table_.GetEntry(cuid, &index_entry) && index_entry.HasSnapshot()) {
    uint64_t old_file_num = index_entry.snapshot_segments[0].file_number;
    auto it = lifecycle_manager_->GetFilePath(old_file_num); // 需要在 LifecycleManager 加这个接口
    if (!it.empty()) {
        old_file_path = it;
        has_old_data = true;
    }
  }
  Status s = MergeAndFlush(cuid, new_data, has_old_data ? old_file_path : "");
  if (!s.ok()) {
    fprintf(stderr, "[HotspotManager] Merge failed for CUID %lu: %s\n", cuid, s.ToString().c_str());
    // 错误处理：由于 new_data 已经从 Buffer 拿出来了，失败会导致数据丢失。
    // 生产环境需要回滚 Buffer。
    return;
  }
  // old data ref--
  if (has_old_data) {
    // 在 updateSnapshot 中的逻辑
  }
}

Status HotspotManager::MergeAndFlush(uint64_t cuid, 
                                     const std::vector<HotEntry>& new_data, 
                                     const std::string& old_file_path) {
  Status s;
  
  // 准备新 SST Writer
  EnvOptions env_options;
  SstFileWriter sst_writer(env_options, db_options_);
  std::string new_file_path = GenerateSstFileName(cuid);
  s = sst_writer.Open(new_file_path);
  if (!s.ok()) return s;

  // 准备旧 SST Reader
  std::unique_ptr<SstFileReader> sst_reader;
  std::unique_ptr<Iterator> iter_old;
  
  if (!old_file_path.empty()) {
    sst_reader.reset(new SstFileReader(db_options_));
    s = sst_reader->Open(old_file_path);
    if (s.ok()) {
      ReadOptions ro;
      iter_old.reset(sst_reader->NewIterator(ro));
      iter_old->SeekToFirst();
    } else {
      fprintf(stderr, "[HotspotManager] Failed to open old SST %s: %s\n", old_file_path.c_str(), s.ToString().c_str());
      return s;
    }
  }

  VectorIterator iter_new(new_data);

  // --- 多路归并 (2-way Merge) & 去重逻辑 ---
  // 规则：New Data 覆盖 Old Data
  
  while (iter_new.Valid() || (iter_old && iter_old->Valid())) {
    bool pick_new = false;
    bool pick_old = false;

    // 1. 确定当前要比较的 Key
    if (iter_new.Valid() && (!iter_old || !iter_old->Valid())) {
      pick_new = true;
    } else if (!iter_new.Valid() && (iter_old && iter_old->Valid())) {
      pick_old = true;
    } else {
      // 两者都 Valid，比较 Key
      // 注意：Old SST 可能包含其他 CUID 的数据 (如果文件是共享的)
      // 需要先检查 iter_old 当前 Key 是否属于目标 CUID
      uint64_t old_cuid = ExtractCUID(iter_old->key());
      if (old_cuid != cuid) {
        // 这里的假设是 SST 内部按 CUID 排序。
        // 如果 old_cuid < cuid，跳过旧数据 (Valid but not match)
        // 如果 old_cuid > cuid，说明旧文件里该 CUID 数据已读完
        if (old_cuid < cuid) {
            iter_old->Next();
            continue; 
        } else {
            // old_cuid > cuid，旧数据耗尽
            pick_new = true;
        }
      } else {
          // CUID 匹配，比较 User Key
          // TODO: 这里的 Key 比较需要使用 UserComparator
          int cmp = iter_new.Key().compare(iter_old->key()); 
          if (cmp < 0) {
            pick_new = true;
          } else if (cmp > 0) {
            pick_old = true;
          } else {
            // Key 相同 (Update)，保留新数据，丢弃旧数据
            pick_new = true;
            iter_old->Next(); // Advance old to skip it
          }
      }
    }

    // 2. 执行写入
    if (pick_new) {
      s = sst_writer.Put(iter_new.Key(), iter_new.Value());
      iter_new.Next();
    } else if (pick_old) {
      // 只有当 old key 属于当前 CUID 时才写入
      if (ExtractCUID(iter_old->key()) == cuid) {
          s = sst_writer.Put(iter_old->key(), iter_old->value());
      }
      iter_old->Next();
    } else {
        // Should not happen
        break;
    }
    
    if (!s.ok()) return s;
  }

  // 完成写入
  ExternalSstFileInfo file_info;
  s = sst_writer.Finish(&file_info);
  if (!s.ok()) return s;

  // 5. 更新索引表 (Metadata Update)
  // 获取 FileNumber (从文件名或 file_info 解析，或者直接生成)
  // 这里简化，假设文件名包含 timestamp 作为 file number
  // 实际上应该用 file_info 的信息或者外部维护 ID
  uint64_t new_file_number = 0; 
  // 解析 file_path 拿到 number，或者在生成 path 时就定好
  // 这里的 GenerateSstFileName 使用了 timestamp，需保持一致
  
  // 注册新文件到生命周期管理
  lifecycle_manager_->RegisterFile(file_info.file_number, new_file_path); // 假设 SstFileWriter 能返回 number，或者我们手动管理

  // 构建 DataSegment
  DataSegment segment;
  segment.file_number = file_info.file_number; // 需确保 SstFileWriter 正确设置了 Sequence/FileNum
  segment.offset = 0;
  segment.length = file_info.file_size;

  // 原子更新 Index: 设置新 Snapshot，清空 Deltas
  std::vector<DataSegment> new_snapshots = {segment};
  index_table_.UpdateSnapshot(cuid, new_snapshots);

  fprintf(stdout, "[HotspotManager] Merge finished. New SST: %s, Size: %lu\n", 
          new_file_path.c_str(), file_info.file_size);

  return Status::OK();
}

Status HotspotManager::FlushBlockToSharedSST(
    std::unique_ptr<HotDataBlock> block,
    std::unordered_map<uint64_t, DataSegment>* output_segments) {
    
  if (!block || block->entries.empty()) return Status::OK();

  // 1. 排序 (按 CUID 聚簇 + Key 排序)
  block->Sort();

  // 2. 准备 Writer
  EnvOptions env_options;
  SstFileWriter sst_writer(env_options, db_options_);
  
  // 文件名使用时间戳
  auto now = std::chrono::system_clock::now();
  uint64_t file_number = std::chrono::duration_cast<std::chrono::microseconds>(
                             now.time_since_epoch()).count();
  std::string file_path = data_dir_ + "/hot_shared_" + std::to_string(file_number) + ".sst";

  Status s = sst_writer.Open(file_path);
  if (!s.ok()) return s;

  lifecycle_manager_->RegisterFile(file_number, file_path);

  // 3. 遍历写入并记录 Segment
  auto& entries = block->entries;
  size_t i = 0;
  while (i < entries.size()) {
    uint64_t current_cuid = entries[i].cuid;
    uint64_t start_offset = sst_writer.FileSize(); // 记录起始偏移
    std::string first_key = entries[i].key;        // 记录 First Key
    
    // 当前 CUID 的所有 Entry
    while (i < entries.size() && entries[i].cuid == current_cuid) {
      s = sst_writer.Put(entries[i].key, entries[i].value);
      if (!s.ok()) return s;
      i++;
    }
    
    uint64_t end_offset = sst_writer.FileSize();
    uint64_t length = end_offset - start_offset;

    if (length > 0) {
      DataSegment segment;
      segment.file_number = file_number;
      segment.offset = start_offset;
      segment.length = length;
      segment.first_key = first_key; // 设置 First Key

      (*output_segments)[current_cuid] = segment;
    }
  }

  // 4. Finish
  ExternalSstFileInfo file_info;
  s = sst_writer.Finish(&file_info);
  if (!s.ok()) return s;
  
  fprintf(stdout, "[HotspotManager] Flushed Shared SST: %s, CUIDs: %lu\n", 
          file_path.c_str(), output_segments->size());

  return Status::OK();
}

void HotspotManager::TriggerBufferFlush() {
    // 轮转 Buffer
    buffer_.RotateBuffer();
    
    // 提取待刷盘 Block
    auto block = buffer_.ExtractBlockToFlush();
    while (block) {
        std::unordered_map<uint64_t, DataSegment> new_segments;
        Status s = FlushBlockToSharedSST(std::move(block), &new_segments);
        
        if (s.ok()) {
            // 批量更新索引
            for (const auto& kv : new_segments) {
                // 简化实现：直接作为 Snapshot 片段追加?
                index_table_.AppendSnapshotSegment(kv.first, kv.second);
            }
        }
        block = buffer_.ExtractBlockToFlush();
    }
}

}  // namespace ROCKSDB_NAMESPACE