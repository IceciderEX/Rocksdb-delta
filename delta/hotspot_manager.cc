//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "delta/hotspot_manager.h"

#include <chrono>
#include <cinttypes>
#include <sstream>

#include "db/dbformat.h"
#include "logging/logging.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/extract_cuid.h"

namespace ROCKSDB_NAMESPACE {

HotspotManager::HotspotManager(const Options& db_options,
                               const std::string& data_dir,
                               const InternalKeyComparator* internal_comparator)
    : db_options_(db_options),
      data_dir_(data_dir),
      internal_comparator_(internal_comparator),
      lifecycle_manager_(std::make_shared<HotSstLifecycleManager>(db_options)),
      buffer_(db_options_.delta_options.hot_data_buffer_threshold_bytes, db_options_.delta_options.hot_data_buffer_shards),
      index_table_(lifecycle_manager_, db_options_.info_log, db_options_.delta_options.sharding_count),
      frequency_table_(db_options_.delta_options.hotspot_scan_threshold,
                       db_options_.delta_options.hotspot_scan_window_sec,
                       db_options_.delta_options.sharding_count),
      delete_table_(db_options_.delta_options.sharding_count) {
  db_options_.env->CreateDirIfMissing(data_dir_);

  // 恢复之前因为宕机等原因遗留的 GDCT 逻辑删除记录
  RecoverGDCT();
  // 初始化持久化日志 AppendableWriter
  InitGDCTLog();
}

uint64_t HotspotManager::ExtractCUID(const Slice& key) {
  // TODO: 根据实际的 Key Schema提取 cuid，这里先假设一波
  if (key.size() < 24) {
    return 0;
  }

  const unsigned char* p =
      reinterpret_cast<const unsigned char*>(key.data()) + 16;

  // Big-Endian Decoding
  uint64_t cuid = (static_cast<uint64_t>(p[0]) << 56) |
                  (static_cast<uint64_t>(p[1]) << 48) |
                  (static_cast<uint64_t>(p[2]) << 40) |
                  (static_cast<uint64_t>(p[3]) << 32) |
                  (static_cast<uint64_t>(p[4]) << 24) |
                  (static_cast<uint64_t>(p[5]) << 16) |
                  (static_cast<uint64_t>(p[6]) << 8) |
                  (static_cast<uint64_t>(p[7]));

  return cuid;
}

bool HotspotManager::RegisterScan(uint64_t cuid, bool is_full_scan,
                                  bool* became_hot) {
  if (cuid == 0) {
    if (became_hot) *became_hot = false;
    return false;
  }

  bool first_time_hot = false;
  bool is_hot = frequency_table_.RecordAndCheckHot(cuid, &first_time_hot);

  // 返回是否首次成为热点
  if (became_hot) {
    *became_hot = first_time_hot;
  }

  if (is_hot && !first_time_hot) {
    // We do not eagerly initialize pending_snapshots_ here anymore.
    // It should be explicitly initialized by PrepareForFullReplace when a
    // FullScan really starts buffering.
  }

  if (is_full_scan) {
    delete_table_.ResetTracking(cuid);
  }
  return is_hot;
}

bool HotspotManager::BufferHotData(uint64_t cuid, const Slice& key,
                                   const Slice& value) {
  {
    std::lock_guard<std::mutex> lock(buffered_cuids_mutex_);
    active_buffered_cuids_.insert(cuid);
  }
  return buffer_.Append(cuid, key, value);
}

bool HotspotManager::PrepareForFullReplace(uint64_t cuid) {
  // Exclusive Lock for Full Scan
  if (!TryLockCuid(cuid)) {
    return false;
  }
  std::lock_guard<std::mutex> lock(pending_mutex_);
  if (pending_snapshots_.find(cuid) == pending_snapshots_.end()) {
    pending_snapshots_[cuid] = std::vector<DataSegment>();
  }
  return true;
}

Status HotspotManager::InterceptDelete(const Slice& key, SequenceNumber seq,
                                       bool sync) {
  uint64_t cuid = ExtractCUID(key);
  if (cuid == 0) return Status::NotSupported("CUID not found");

  bool newly_deleted = false;
  // 在 GDCT 中查询是否该 cuid 已经被追踪为热点
  bool tracked = delete_table_.MarkDeleted(cuid, seq, &newly_deleted);

  if (tracked) {
    if (newly_deleted) {
      // 第一次 delete
      return PersistDelete(cuid, seq, sync);
    }
    return Status::OK();
  }
  // CUID 不在热点管理范围内
  return Status::NotSupported("Not a hot CUID");
}

Status HotspotManager::InitGDCTLog() {
  std::lock_guard<std::mutex> lock(gdct_log_mutex_);
  if (gdct_log_writer_) return Status::OK();

  std::string log_dir = data_dir_ + "/hot_shared_";
  db_options_.env->CreateDirIfMissing(log_dir);
  std::string log_path = log_dir + "/gdct.log";

  EnvOptions env_options;
  Status s = db_options_.env->ReopenWritableFile(log_path, &gdct_log_writer_,
                                                 env_options);
  if (s.IsNotSupported()) {
    std::cout << "[HotspotManager] ReopenWritableFile is not supported, using "
                 "NewWritableFile"
              << std::endl;
    // In a real environment fallback would go here depending on FS
  }
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "[HotspotManager] Failed to open GDCT log file: %s",
                    s.ToString().c_str());
  }
  return s;
}

Status HotspotManager::FlushGDCTLogBuffer() {
  if (pending_gdct_records_.load(std::memory_order_relaxed) == 0) {
    return Status::OK();
  }

  std::vector<std::pair<uint64_t, SequenceNumber>> local_buffer;
  {
    std::lock_guard<std::mutex> lock(gdct_append_mutex_);
    if (gdct_append_buffer_.empty()) {
      pending_gdct_records_.store(0, std::memory_order_relaxed);
      return Status::OK();
    }
    local_buffer = std::move(gdct_append_buffer_);
    pending_gdct_records_.store(0, std::memory_order_relaxed);
  }

  last_gdct_flush_time_us_.store(db_options_.env->NowMicros(),
                                 std::memory_order_relaxed);

  std::lock_guard<std::mutex> lock(gdct_log_mutex_);
  if (!gdct_log_writer_) {
    InitGDCTLog();
    if (!gdct_log_writer_) return Status::IOError("No GDCT log writer");
  }

  Status s;
  for (const auto& record : local_buffer) {
    char buffer[16];
    EncodeFixed64(buffer, record.first);
    EncodeFixed64(buffer + 8, record.second);
    s = gdct_log_writer_->Append(Slice(buffer, 16));
    if (!s.ok()) break;
  }

  if (s.ok()) {
    s = gdct_log_writer_->Sync();
  }
  return s;
}

Status HotspotManager::PersistDelete(uint64_t cuid, SequenceNumber seq,
                                     bool sync) {
  if (sync) {
    // 同步写
    FlushGDCTLogBuffer();  // 先刷掉积压的
    std::lock_guard<std::mutex> lock(gdct_log_mutex_);
    if (!gdct_log_writer_) InitGDCTLog();
    if (!gdct_log_writer_) return Status::IOError("No GDCT log writer");

    char buffer[16];
    EncodeFixed64(buffer, cuid);
    EncodeFixed64(buffer + 8, seq);
    Status s = gdct_log_writer_->Append(Slice(buffer, 16));
    if (s.ok()) s = gdct_log_writer_->Sync();
    return s;
  } else {
    // 异步写 加到内存缓冲
    std::lock_guard<std::mutex> lock(gdct_append_mutex_);
    gdct_append_buffer_.push_back({cuid, seq});
    pending_gdct_records_.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
  }
}

void HotspotManager::RecoverGDCT() {
  std::string log_dir = data_dir_ + "/hot_shared_";
  std::string log_path = log_dir + "/gdct.log";
  std::unique_ptr<SequentialFile> file;
  EnvOptions env_options;

  Status s = db_options_.env->NewSequentialFile(log_path, &file, env_options);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return;  // 没有日志文件是正常的
    }
    ROCKS_LOG_WARN(db_options_.info_log,
                   "[HotspotManager] Failed to open GDCT log for recovery: %s",
                   s.ToString().c_str());
    return;
  }

  size_t recovered_count = 0;
  char buffer[16];
  Slice result;

  while (true) {
    s = file->Read(16, &result, buffer);
    if (!s.ok() || result.size() < 16) {
      break;
    }

    uint64_t cuid = DecodeFixed64(result.data());
    SequenceNumber seq = DecodeFixed64(result.data() + 8);

    delete_table_.MarkDeleted(cuid, seq);
    recovered_count++;
  }

  ROCKS_LOG_INFO(
      db_options_.info_log,
      "[HotspotManager] Recovered %zu CUID delete records from GDCT log.",
      recovered_count);
}

void HotspotManager::CompactAndFlushGDCTLogIfNeeded() {
  uint64_t now = db_options_.env->NowMicros();

  // 没有积压数据 && time interval >= 30s
  if (pending_gdct_records_.load(std::memory_order_relaxed) <
          db_options_.delta_options.gdct_flush_threshold_records &&
      now - last_gdct_flush_time_us_.load(std::memory_order_relaxed) <
          db_options_.delta_options.gdct_flush_interval_us) {
    return;
  }
  FlushGDCTLogBuffer();
  // 检查日志重写 (Compact) && time interval >= 600s
  if (now - last_gdct_compact_time_us_.load(std::memory_order_relaxed) <
      db_options_.delta_options.gdct_compact_interval_us) {
    return;
  }
  last_gdct_compact_time_us_.store(now, std::memory_order_relaxed);

  std::string log_dir = data_dir_ + "/hot_shared_";
  std::string log_path = log_dir + "/gdct.log";

  uint64_t file_size = 0;
  Status s = db_options_.env->GetFileSize(log_path, &file_size);
  if (!s.ok() ||
      file_size < db_options_.delta_options.gdct_log_compact_size) {
    return;
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[HotspotManager] GDCT log size %" PRIu64
                 " bytes, starting compaction.",
                 file_size);

  std::string new_log_path = log_dir + "/gdct.log.new";
  std::unique_ptr<WritableFile> new_writer;
  EnvOptions env_options;
  s = db_options_.env->NewWritableFile(new_log_path, &new_writer, env_options);
  if (!s.ok()) return;

  auto deleted_cuids = delete_table_.GetAllDeletedCuids();

  for (const auto& pair : deleted_cuids) {
    char buffer[16];
    EncodeFixed64(buffer, pair.first);
    EncodeFixed64(buffer + 8, pair.second);
    s = new_writer->Append(Slice(buffer, 16));
    if (!s.ok()) break;
  }

  if (s.ok()) s = new_writer->Sync();
  if (s.ok())
    s = new_writer->Close();
  else
    new_writer->Close();

  if (s.ok()) {
    std::lock_guard<std::mutex> lock(gdct_log_mutex_);
    if (gdct_log_writer_) {
      gdct_log_writer_->Close();
      gdct_log_writer_.reset();
    }
    s = db_options_.env->RenameFile(new_log_path, log_path);
    if (s.ok()) {
      db_options_.env->ReopenWritableFile(log_path, &gdct_log_writer_,
                                          env_options);
      ROCKS_LOG_INFO(db_options_.info_log,
                     "[HotspotManager] Successfully compacted GDCT log.");
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

bool HotspotManager::ShouldTriggerScanAsCompaction(uint64_t cuid) {
  HotIndexEntry entry;
  if (!index_table_.GetEntry(cuid, &entry)) {
    // a)	当前热点CUid无Snapshot。
    return true;
  }
  // b)已有Snapshot，且新增的Deltas片段数量超过阈值。
  if (!entry.HasSnapshot() ||
      entry.deltas.size() >
          db_options_.delta_options.sac_delta_count_threshold) {
    return true;
  }
  return false;
}

Status HotspotManager::FlushBlockToSharedSST(
    std::shared_ptr<HotDataBlock> block,
    std::unordered_map<uint64_t, DataSegment>* output_segments) {
  if (!block || block->buckets.empty()) return Status::OK();

  // keysort: already in RotateBuffer()

  EnvOptions env_options;
  SstFileWriter sst_writer(env_options, db_options_);

  // 文件名使用时间戳
  auto now = std::chrono::system_clock::now();
  uint64_t file_number = std::chrono::duration_cast<std::chrono::microseconds>(
                             now.time_since_epoch())
                             .count();
  std::string file_path =
      data_dir_ + "/hot_shared_" + std::to_string(file_number) + ".sst";

  Status s = sst_writer.Open(file_path);
  if (!s.ok()) return s;

  // link file，让 snapshot 的 iterator 能够正确找到文件
  // link_path = db_root / <file_number>.sst
  // data_dir_ = dbname_ / hotspot_data
  std::string db_root = data_dir_;
  size_t last_slash = db_root.find_last_of("/\\");
  if (last_slash != std::string::npos) {
    db_root = db_root.substr(0, last_slash);
  }
  std::string link_path = db_root + "/" + std::to_string(file_number) + ".sst";

  s = db_options_.env->LinkFile(file_path, link_path);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "[HotspotManager] Failed to create link %s -> %s: %s",
                    file_path.c_str(), link_path.c_str(), s.ToString().c_str());
  }

  lifecycle_manager_->RegisterFile(file_number, file_path, link_path);

  // 3. 按 CUID 顺序写入数据
  // DataBlock 内部已经按 Bucket 组织，按 CUID 升序遍历 Bucket
  std::vector<uint64_t> cuids;
  for (const auto& pair : block->buckets) {
    cuids.push_back(pair.first);
  }
  std::sort(cuids.begin(), cuids.end());

  for (uint64_t current_cuid : cuids) {
    const auto& bucket_entries = block->buckets[current_cuid];
    if (bucket_entries.empty()) continue;

    const HotEntry& first_entry = bucket_entries[0];
    std::string segment_first_key = first_entry.key;
    std::string segment_last_key;
    std::string last_written_key_in_segment;
    bool is_first_entry_in_segment = true;
    int written_count = 0;

    for (const HotEntry& entry : bucket_entries) {
      Slice entry_key(entry.key);
      Slice current_user_key_slice = ExtractUserKey(entry_key);
      std::string current_user_key = current_user_key_slice.ToString();

      // 去重
      if (!is_first_entry_in_segment &&
          current_user_key == last_written_key_in_segment) {
        continue;
      }

      Slice entry_value(entry.value);
      s = sst_writer.Put(current_user_key_slice, entry_value);
      if (!s.ok()) return s;

      last_written_key_in_segment = current_user_key;
      segment_last_key = entry_key.ToString();
      written_count++;
      is_first_entry_in_segment = false;
    }

    if (written_count > 0) {
      DataSegment segment;
      segment.file_number = file_number;
      segment.first_key = segment_first_key;
      segment.last_key = segment_last_key;
      (*output_segments)[current_cuid] = segment;
    }
  }

  // 4. Finish
  ExternalSstFileInfo file_info;
  s = sst_writer.Finish(&file_info);
  if (!s.ok()) {
    return s;
  }

  uint64_t actual_file_size = file_info.file_size;
  for (auto& kv : *output_segments) {
    kv.second.file_size = actual_file_size;
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[HotspotManager] Flushed Shared SST: %s, CUIDs: %zu",
                 file_path.c_str(), output_segments->size());

  return Status::OK();
}

void HotspotManager::TriggerBufferFlush() {
  // 防止多次 flush 
  std::lock_guard<std::mutex> flush_lock(flush_mutex_);
  // 轮转 Buffer，传入 InternalKeyComparator 保证排序严格有序
  if (!buffer_.RotateBuffer(internal_comparator_)) {
    return;
  }

  // 提取待刷盘 Block
  // 先获取 block 执行刷盘，成功后再从队列移除
  // 防止 reader 再 flush 过程中看不到数据
  ROCKS_LOG_DEBUG(db_options_.info_log,
                  "[HotspotManager] Buffer rotated. Preparing to flush block.");
  auto block = buffer_.GetFrontBlockForFlush();
  while (block) {
    std::unordered_map<uint64_t, DataSegment> new_segments;
    // to share SST
    Status s = FlushBlockToSharedSST(std::move(block), &new_segments);

    if (s.ok()) {
      for (const auto& kv : new_segments) {
        uint64_t cuid = kv.first;
        const DataSegment& real_segment = kv.second;

        // 先检查 pending_snapshots_，再尝试 PromoteSnapshot
        // 如果该 CUID 有正在进行的 full Scan（pending_snapshots_ 中有注册），
        // 将 SST 放入 pending，由 FinalizeScanAsCompaction 提交
        bool added_to_pending = false;
        {
          std::lock_guard<std::mutex> lock(pending_mutex_);
          auto it = pending_snapshots_.find(cuid);
          if (it != pending_snapshots_.end()) {
            if (cuid == 1003) {
              fprintf(stderr, "[DIAG_FLUSH] CUID 1003 Pending SST added: file %lu, range [%s - %s]\n", 
                      real_segment.file_number, FormatKeyDisplay(real_segment.first_key).c_str(), 
                      FormatKeyDisplay(real_segment.last_key).c_str());
            }
            it->second.push_back(real_segment);
            added_to_pending = true;
          }
        }

        if (!added_to_pending) {
          // 无活跃 Scan，尝试 PromoteSnapshot（将 {-1} 内存段替换为真实 SST）
          bool promoted = index_table_.PromoteSnapshot(
              cuid, real_segment, &buffer_, internal_comparator_);
          if (cuid == 1003) {
            fprintf(stderr, "[DIAG_FLUSH] CUID 1003 PromoteSnapshot result: %s, file: %lu\n", 
                    promoted ? "SUCCESS" : "FAILED", real_segment.file_number);
          }
          if (!promoted) {
            // 既无活跃 Scan 也无 {-1} 段，强制 Append
            ROCKS_LOG_WARN(db_options_.info_log,
                           "[HotspotManager] No active scan or {-1} segment "
                           "for CUID %" PRIu64
                           ". Appending snapshot segment directly.",
                           cuid);
            index_table_.AppendSnapshotSegment(cuid, real_segment);
          }
        }
      }
    } else {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "[HotspotManager] Failed to flush shared block: %s",
                      s.ToString().c_str());
    }
    // 刷盘和index更新完成之后再移除
    if (block) {
        fprintf(stderr, "[DIAG_POP] Popping block %p from buffer queue\n", block.get());
    }
    buffer_.PopFrontBlockAfterFlush();
    block = buffer_.GetFrontBlockForFlush();
  }
}

void HotspotManager::FinalizeScanAsCompaction(
    uint64_t cuid, const std::unordered_set<uint64_t>& visited_files,
    const std::string& scan_first_key, const std::string& scan_last_key) {
  if (cuid == 0) return;
  auto unlock_guard = std::shared_ptr<void>( 
    nullptr, [this, cuid](void*) { this->UnlockCuid(cuid); });

  std::vector<DataSegment> final_segments;

  // Scan 过程中产生的 Pending SSTs【在 scan 的过程中flush的】
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_snapshots_.find(cuid);
    if (it != pending_snapshots_.end()) {
      final_segments = std::move(it->second);
      pending_snapshots_.erase(it);

      // 处理由于并发或历史 bufferdata 残留的 SST seg 范围重叠问题
      for (auto seg_it = final_segments.begin(); seg_it != final_segments.end();) {
        // 只以这次 full scan 的 keyrange 为准
        if (internal_comparator_->Compare(seg_it->first_key, scan_first_key) <
            0) {
          seg_it->first_key = scan_first_key;
        }
        if (internal_comparator_->Compare(seg_it->last_key, scan_last_key) >
            0) {
          seg_it->last_key = scan_last_key;
        }

        // 如果裁剪后发现 seg first >= last，这段 segment 无效
        if (internal_comparator_->Compare(seg_it->first_key,
                                          seg_it->last_key) >= 0) {
          seg_it = final_segments.erase(seg_it);
        } else {
          ++seg_it;
        }
      }
    }
  }

  bool has_buffered_data = false;
  {
    std::lock_guard<std::mutex> lock(buffered_cuids_mutex_);
    auto it = active_buffered_cuids_.find(cuid);
    if (it != active_buffered_cuids_.end()) {
      has_buffered_data = true;
      active_buffered_cuids_.erase(it);  // 清理状态
    }
  }

  // 防止空scan写入snapshot的情况
  if (final_segments.empty() && !has_buffered_data) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[HotspotManager] FinalizeScanAsCompaction for CUID %" PRIu64
                   " skipped: no pending SSTs and no buffered data."
                   " (Expected for Metadata Scans)",
                   cuid);
    return;
  }

  // 防止 buffer data 的 segment keyrange 重叠
  std::string tail_min;
  if (final_segments.empty()) {
    tail_min = scan_first_key;
  } else {
    // buffer minkey=前一个物理 SST 的结尾
    tail_min = final_segments.back().last_key;
  }

  // 确保拼接逻辑不会发生区间倒置
  bool has_valid_tail =
      internal_comparator_->Compare(tail_min, scan_last_key) <= 0;
  if (has_buffered_data && has_valid_tail) {
    DataSegment tail_segment;
    tail_segment.file_number = static_cast<uint64_t>(-1);  // 标记为内存段
    tail_segment.first_key = tail_min;
    tail_segment.last_key = scan_last_key;

    final_segments.push_back(tail_segment);
  } else if (!has_valid_tail || !has_buffered_data) {
    ROCKS_LOG_WARN(
        db_options_.info_log,
        "[HotspotManager] Skipped creating tail segment for CUID %" PRIu64
        ". Has buffered data: %d, valid_tail: %d, Pending SSTs: %zu",
        cuid, has_buffered_data ? 1 : 0, has_valid_tail ? 1 : 0,
        final_segments.size());

    if (final_segments.empty()) {
      return;
    }
  }

  // Only update snapshot if we actually have segments to update with
  if (final_segments.empty()) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[HotspotManager] FinalizeScanAsCompaction for CUID %" PRIu64
                   " skipped: final_segments is empty.",
                   cuid);
    return;
  }

  // 【debug】验证拼接出来的 final_segments 是不是重叠的
  if (final_segments.size() > 1) {
    for (size_t i = 0; i < final_segments.size() - 1; ++i) {
      const auto& s1 = final_segments[i];
      const auto& s2 = final_segments[i+1];
      
      // 检测方法 A: 顺序检测 (i+1 的起点应该 >= i 的起点)
      bool unsorted = internal_comparator_->Compare(s1.first_key, s2.first_key) > 0;
      
      // 检测方法 B: 重叠检测 (i 的终点不能跨过 i+1 的起点)
      bool overlapping = internal_comparator_->Compare(s1.last_key, s2.first_key) > 0;
      
      if (unsorted || overlapping) {
        fprintf(stderr, "\n[DIAGNOSE_FATAL] FinalizeScanAsCompaction Inconsistency detected!\n");
        fprintf(stderr, "CUID: %lu, Issue: %s\n", cuid, unsorted ? "UNSORTED" : "OVERLAPPING");
        fprintf(stderr, "Seg[%zu] [%s - %s], file: %lu\n", 
                i, FormatKeyDisplay(s1.first_key).c_str(), 
                FormatKeyDisplay(s1.last_key).c_str(), s1.file_number);
        fprintf(stderr, "Seg[%zu] [%s - %s], file: %lu\n", 
                i+1, FormatKeyDisplay(s2.first_key).c_str(), 
                FormatKeyDisplay(s2.last_key).c_str(), s2.file_number);
        fprintf(stderr, "--------------\n");
      }
    }
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[HotspotManager] FinalizeScanAsCompaction for CUID %" PRIu64
                 " updating snapshot with %zu segments.",
                 cuid, final_segments.size());

  // 更新这个 cuid 的 Snapshot
  index_table_.UpdateSnapshot(cuid, final_segments);
  index_table_.MarkDeltasAsObsolete(cuid, visited_files);

  // fprintf(stdout, "[HotspotManager] Finalized CUID %lu. Snapshot has %zu
  // segments (incl tail).\n",
  //         cuid, final_segments.size());
  // DebugDump("AFTER_SCAN_FINALIZE_CUID_" + std::to_string(cuid));
}

ScanAsCompactionStrategy HotspotManager::EvaluateScanAsCompactionStrategy(
    uint64_t cuid, bool is_full_scan, const std::string& scan_first_key,
    const std::string& scan_last_key, size_t* out_involved_delta_count) {
  // 全版本 Scan 使用 FullReplace 策略
  if (is_full_scan) {
    if (out_involved_delta_count) {
      *out_involved_delta_count = 0;
    }
    return ScanAsCompactionStrategy::kFullReplace;
  }

  // 小 Scan: 查看涉及多少个 delta segments
  size_t delta_count = CountInvolvedDeltas(cuid, scan_first_key, scan_last_key);
  if (out_involved_delta_count) {
    *out_involved_delta_count = delta_count;
  }

  // 根据阈值决定策略
  if (delta_count >= db_options_.delta_options.delta_merge_threshold) {
    return ScanAsCompactionStrategy::kPartialMerge;
  }

  return ScanAsCompactionStrategy::kNoAction;
}

size_t HotspotManager::CountInvolvedDeltas(uint64_t cuid,
                                           const std::string& first_key,
                                           const std::string& last_key) {
  return index_table_.CountOverlappingDeltas(cuid, first_key, last_key);
}

void HotspotManager::FinalizeScanAsCompactionWithStrategy(
    uint64_t cuid, ScanAsCompactionStrategy strategy,
    const std::string& scan_first_key, const std::string& scan_last_key,
    const std::unordered_set<uint64_t>& visited_files,
    const std::vector<std::pair<std::string, std::string>>& scan_data) {
  if (cuid == 0) return;

  switch (strategy) {
    case ScanAsCompactionStrategy::kNoAction:
      return;
    case ScanAsCompactionStrategy::kFullReplace:
      FinalizeScanAsCompaction(cuid, visited_files, scan_first_key,
                               scan_last_key);
      // std::cout << "[HotspotManager] Finalized CUID " << cuid
      //           << " with FullReplace strategy." << std::endl;
      break;
    case ScanAsCompactionStrategy::kPartialMerge:
      EnqueuePartialMerge(cuid, scan_first_key, scan_last_key, scan_data);
      // std::cout << "[HotspotManager] Enqueued CUID " << cuid
      //           << " for partial merge." << std::endl;
      break;
  }
}

// ----------------- L0Compaction --------------------

void HotspotManager::UpdateCompactionDelta(
    uint64_t cuid, const std::vector<uint64_t>& input_files,
    uint64_t output_file_number, const std::string& first_key,
    const std::string& last_key) {
  if (first_key.empty()) return;

  DataSegment seg;
  seg.file_number = output_file_number;
  seg.first_key = first_key;
  seg.last_key = last_key;

  index_table_.UpdateDeltaIndex(cuid, input_files, seg);
}

bool HotspotManager::ShouldSkipObsoleteDelta(
    uint64_t cuid, const std::vector<uint64_t>& input_files) {
  return index_table_.IsDeltaObsolete(cuid, input_files);
}

void HotspotManager::CleanUpMetadataAfterCompaction(
    const std::unordered_set<uint64_t>& involved_cuids,
    const std::vector<uint64_t>& input_files) {
  if (input_files.empty() || involved_cuids.empty()) return;

  // 步骤d：热点索引表的处理
  index_table_.RemoveObsoleteDeltasForCUIDs(involved_cuids, input_files);

  // 步骤c：已删除cuid hotdata文件的处理
  for (uint64_t cuid : involved_cuids) {
    delete_table_.UntrackFiles(cuid, input_files);
  }
}

bool HotspotManager::IsHot(uint64_t cuid) {
  return frequency_table_.IsHot(cuid);
}

// --------------------- HotspotManager Iterator --------------------- //
InternalIterator* HotspotManager::NewBufferIterator(
    uint64_t cuid, const InternalKeyComparator* icmp) {
  if (cuid == 1003) {
      fprintf(stderr, "[DIAG_MGR] Reader requesting NewBufferIterator for 1003\n");
  }
  return buffer_.NewIterator(cuid, icmp);
}

// --------------------- Pending Init CUID Queue --------------------- //
void HotspotManager::EnqueueForInitScan(uint64_t cuid) {
  std::lock_guard<std::mutex> lock(pending_init_mutex_);
  // 避免重复添加
  for (uint64_t c : pending_init_cuids_) {
    if (c == cuid) return;
  }
  pending_init_cuids_.push_back(cuid);
  ROCKS_LOG_INFO(
      db_options_.info_log,
      "[HotspotManager] Enqueued CUID %" PRIu64 " for initial full scan", cuid);
}

std::vector<uint64_t> HotspotManager::PopPendingInitCuids() {
  std::lock_guard<std::mutex> lock(pending_init_mutex_);
  std::vector<uint64_t> result;
  for (auto it = pending_init_cuids_.begin();
       it != pending_init_cuids_.end();) {
    if (TryLockCuid(*it)) {
      result.push_back(*it);
      it = pending_init_cuids_.erase(it);
    } else {
      ++it;
    }
  }
  return result;
}

bool HotspotManager::HasPendingInitCuids() const {
  std::lock_guard<std::mutex> lock(pending_init_mutex_);
  return !pending_init_cuids_.empty();
}

// --------------------- Metadata Scan Queue --------------------- //
void HotspotManager::EnqueueMetadataScan(uint64_t cuid) {
  std::lock_guard<std::mutex> lock(pending_metadata_mutex_);
  for (uint64_t c : pending_metadata_scans_) {
    if (c == cuid) return;
  }
  pending_metadata_scans_.push_back(cuid);
  // fprintf(stdout,
  //         "[HotspotManager] Enqueued CUID %lu for metadata ref-count scan\n",
  //         cuid);
}

std::vector<uint64_t> HotspotManager::PopPendingMetadataScans() {
  std::lock_guard<std::mutex> lock(pending_metadata_mutex_);
  std::vector<uint64_t> result = std::move(pending_metadata_scans_);
  pending_metadata_scans_.clear();
  return result;
}

bool HotspotManager::HasPendingMetadataScans() const {
  std::lock_guard<std::mutex> lock(pending_metadata_mutex_);
  return !pending_metadata_scans_.empty();
}

// --------------------- Partial Merge Queue --------------------- //

void HotspotManager::EnqueuePartialMerge(
    uint64_t cuid, const std::string& scan_first_key,
    const std::string& scan_last_key,
    const std::vector<std::pair<std::string, std::string>>& scan_data) {
  std::lock_guard<std::mutex> lock(partial_merge_mutex_);
  // 避免重复添加相同 cuid 的任务?
  for (auto& task : partial_merge_queue_) {
    if (task.cuid == cuid) {
      // 已经有一个队列了，暂时忽略本次小的 scan触发，或者也可以用更大的
      // range更新
      return;
    }
  }
  partial_merge_queue_.push_back(
      {cuid, scan_first_key, scan_last_key, scan_data});
  ROCKS_LOG_INFO(db_options_.info_log,
                 "[HotspotManager] Enqueued PartialMerge for CUID %" PRIu64
                 ", range [%zu, %zu], %zu pairs",
                 cuid, scan_first_key.size(), scan_last_key.size(),
                 scan_data.size());
}

bool HotspotManager::HasPendingPartialMerge() const {
  std::lock_guard<std::mutex> lock(partial_merge_mutex_);
  return !partial_merge_queue_.empty();
}

bool HotspotManager::PopPendingPartialMerge(PartialMergePendingTask* task) {
  std::lock_guard<std::mutex> lock(partial_merge_mutex_);
  if (partial_merge_queue_.empty()) {
    return false;
  }

  for (auto it = partial_merge_queue_.begin(); it != partial_merge_queue_.end();
       ++it) {
    if (TryLockCuid(it->cuid)) {
      *task = std::move(*it);
      partial_merge_queue_.erase(it);
      return true;
    }
  }
  return false;
}

bool HotspotManager::TryLockCuid(uint64_t cuid) {
  std::lock_guard<std::mutex> lock(in_progress_mutex_);
  if (in_progress_cuids_.count(cuid)) {
    return false;
  }
  in_progress_cuids_.insert(cuid);
  return true;
}

void HotspotManager::UnlockCuid(uint64_t cuid) {
  std::lock_guard<std::mutex> lock(in_progress_mutex_);
  in_progress_cuids_.erase(cuid);
}

}  // namespace ROCKSDB_NAMESPACE