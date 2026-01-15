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
    
    bool is_hot = frequency_table_.RecordAndCheckHot(cuid);
    if (is_hot) {
        // 初始化 pending 列表
        if (ShouldTriggerScanAsCompaction(cuid)) {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            if (pending_snapshots_.find(cuid) == pending_snapshots_.end()) {
                pending_snapshots_[cuid] = std::vector<DataSegment>();
            }
        }
    }
    return is_hot;
}

bool HotspotManager::BufferHotData(uint64_t cuid, const Slice& key, const Slice& value) {
  return buffer_.Append(cuid, key, value);
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

Status HotspotManager::FlushBlockToSharedSST(
    std::unique_ptr<HotDataBlock> block,
    std::unordered_map<uint64_t, DataSegment>* output_segments) {
    
  if (!block || block->entries.empty()) return Status::OK();

  // 1. 排序 (按 CUID 聚簇 + Key 排序)
  block->Sort();

  // 2. 准备 Writer
  // 确认一下 VersionSet？【不会进入】
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

    std::string last_written_key;
    bool is_first_entry_in_segment = true;

    uint64_t logical_size = 0; 
    int written_count = 0;
    
    // 当前 CUID 的所有 Entry
    while (i < entries.size() && entries[i].cuid == current_cuid) {
      const auto& current_key = entries[i].key;
      if (!is_first_entry_in_segment && current_key == last_written_key) {
          i++;
          continue; 
      }
      s = sst_writer.Put(entries[i].key, entries[i].value);
      if (!s.ok()) return s;

      logical_size += current_key.size() + entries[i].value.size();
      last_written_key = current_key;

      logical_size += current_key.size() + entries[i].value.size();
      written_count++;
      is_first_entry_in_segment = false;
      i++;
    }
    
    uint64_t end_offset = sst_writer.FileSize();
    uint64_t physical_length = end_offset - start_offset;

    if (written_count > 0) {
      DataSegment segment;
      segment.file_number = file_number;
      segment.offset = start_offset;
      
      segment.length = (physical_length > 0) ? physical_length : logical_size;
      segment.first_key = first_key; 

      (*output_segments)[current_cuid] = segment;
    }
  }

  // 4. Finish
  ExternalSstFileInfo file_info;
  s = sst_writer.Finish(&file_info);
  if (!s.ok()) {
    return s;
  }
  
  fprintf(stdout, "[HotspotManager] Flushed Shared SST: %s, CUIDs: %lu\n", 
          file_path.c_str(), output_segments->size());

  return Status::OK();
}

void HotspotManager::TriggerBufferFlush() {
    // 轮转 Buffer
    if (!buffer_.RotateBuffer()) {
        return;
    }
    
    // 提取待刷盘 Block
    auto block = buffer_.ExtractBlockToFlush();
    while (block) {
        std::unordered_map<uint64_t, DataSegment> new_segments;
        // to share SST
        Status s = FlushBlockToSharedSST(std::move(block), &new_segments);
        
        if (s.ok()) {
            for (const auto& kv : new_segments) {
                uint64_t cuid = kv.first;
                const DataSegment& real_segment = kv.second;
                // 直接作为 Snapshot 片段追加?
                // index_table_.AppendSnapshotSegment(kv.first, kv.second);
                auto it = pending_snapshots_.find(cuid);
                if (it != pending_snapshots_.end()) {
                    // Case A: Scan 过程中触发的 Flush, Finalize 时一起提交
                    it->second.push_back(real_segment);
                } else {
                    // Case B: scan 之后被其他cuid数据填满 flush，
                    // 检查 Index 中 {-1} 的记录
                    bool promoted = index_table_.PromoteSnapshot(cuid, real_segment);
                    if (!promoted) {
                        // 如果没有 -1?
                        index_table_.AppendSnapshotSegment(kv.first, kv.second);
                    }
                }
            }
        } else {
          fprintf(stderr, "[HotspotManager] Failed to flush shared block: %s\n", 
                    s.ToString().c_str());
        }
        block = buffer_.ExtractBlockToFlush();
    }
}

void HotspotManager::FinalizeScanAsCompaction(uint64_t cuid) {
    if (cuid == 0) return;

    std::vector<DataSegment> final_segments;

    // Scan 过程中产生的 Pending SSTs
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_snapshots_.find(cuid);
        if (it != pending_snapshots_.end()) {
            final_segments = it->second;
            pending_snapshots_.erase(it); 
        }
    }

    // tail segment
    DataSegment tail_segment;
    tail_segment.file_number = static_cast<uint64_t>(-1);
    tail_segment.offset = 0; 
    tail_segment.length = 0; 
    
    final_segments.push_back(tail_segment);

    // 更新这个 cuid 的 Snapshot
    index_table_.UpdateSnapshot(cuid, final_segments);

    index_table_.MarkDeltasAsObsolete(cuid);
    
    // fprintf(stdout, "[HotspotManager] Finalized CUID %lu. Snapshot has %zu segments (incl tail).\n", 
    //         cuid, final_segments.size());
}

void HotspotManager::UpdateCompactionDelta(uint64_t cuid, 
                                           const std::vector<uint64_t>& input_files,
                                           uint64_t output_file_number,
                                           uint64_t offset,
                                           uint64_t length) {
    if (length == 0) return;
    
    DataSegment seg;
    seg.file_number = output_file_number;
    seg.offset = offset;
    seg.length = length;
    // seg.first_key = ?; // TODO: 怎么提取？

    index_table_.UpdateDeltaIndex(cuid, input_files, seg);
}

bool HotspotManager::IsHot(uint64_t cuid) {
  return frequency_table_.IsHot(cuid);
}

}  // namespace ROCKSDB_NAMESPACE