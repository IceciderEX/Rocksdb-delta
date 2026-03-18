// delta/global_delete_count_table.cc

#include "delta/global_delete_count_table.h"

namespace ROCKSDB_NAMESPACE {

bool GlobalDeleteCountTable::TrackPhysicalUnit(uint64_t cuid, uint64_t phys_id) {
  auto& shard = GetShard(cuid);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto& entry = shard.table[cuid]; // Lazy Init
  
  // count++
  auto it = std::lower_bound(entry.tracked_phys_ids.begin(), 
                             entry.tracked_phys_ids.end(), 
                             phys_id);
  if (it == entry.tracked_phys_ids.end() || *it != phys_id) {
    entry.tracked_phys_ids.insert(it, phys_id);
    entry.ref_count++;
    return true;
  }
  return false;
}

void GlobalDeleteCountTable::ResetTracking(uint64_t cuid) {
    auto& shard = GetShard(cuid);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    auto it = shard.table.find(cuid);
    if (it != shard.table.end()) {
      it->second.ref_count = 0;
      it->second.tracked_phys_ids.clear();
    }
}

// void GlobalDeleteCountTable::UntrackPhysicalUnit(uint64_t cuid, uint64_t phys_id) {
//   std::unique_lock<std::shared_mutex> lock(mutex_);
//   auto it = table_.find(cuid);
//   if (it != table_.end()) {
//     it->second.tracked_phys_ids.erase(phys_id);
//     it->second.ref_count--;
//     // 如果计数归零且已标记删除的清理？
//     // if (it->second.is_deleted && it->second.tracked_phys_ids.empty()) {
//     //     table_.erase(it);
//     // }
//   }
// }

// 用于 L0Compaction 对 delete cuid 的清理
// void GlobalDeleteCountTable::UntrackFiles(uint64_t cuid, const std::vector<uint64_t>& file_ids) {
//   std::unique_lock<std::shared_mutex> lock(mutex_);
//   auto it = table_.find(cuid);
//   if (it != table_.end()) {
//     // 遍历本次 Compaction 的所有输入文件
//     for (uint64_t fid : file_ids) {
//       it->second.tracked_phys_ids.erase(fid);
//     }
//     // 检查是否归零且标记删除，如果是则清理条目
//     if (it->second.tracked_phys_ids.empty() && it->second.is_deleted) {
//       table_.erase(it);
//     }
//   }
// }

void GlobalDeleteCountTable::UntrackFiles(uint64_t cuid, const std::vector<uint64_t>& file_ids) {
  auto& shard = GetShard(cuid);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  if (it == shard.table.end()) return;

  auto& entry = it->second;
  auto& ids = entry.tracked_phys_ids; // 这是用于校验的 Vector

  for (uint64_t fid : file_ids) {
      // 使用二分查找在校验 Vector 中寻找文件 ID
      auto pos = std::lower_bound(ids.begin(), ids.end(), fid);
      if (pos != ids.end() && *pos == fid) {
          ids.erase(pos);
          entry.ref_count--; // 同步扣减逻辑计数
      }
  }

  // 检查是否需要清理条目 (引用归零 且 标记删除)
  if (entry.ref_count <= 0 && entry.is_deleted) {
      shard.table.erase(it);
  }
}

void GlobalDeleteCountTable::ApplyCompactionChange(
                             uint64_t cuid, 
                             int32_t input_count, int32_t output_count,
                             const std::vector<uint64_t>& input_files,
                             uint64_t output_file) {
    
  auto& shard = GetShard(cuid);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  
  // 如果这个 CUID 根本没被追踪过 (比如 Scan 还没发生)，但 Compaction 却生成了它
  // 这种情况理论上少见，但也需要处理
  if (it == shard.table.end()) {
     if (output_count > 0) {
         auto& new_entry = shard.table[cuid];
         new_entry.ref_count += output_count;
         if (output_file != 0) new_entry.tracked_phys_ids.push_back(output_file);
     }
     return;
  }

  auto& entry = it->second;

  entry.ref_count = entry.ref_count - input_count + output_count;

  // 2.1 移除参与 Compaction 的旧文件
  for (uint64_t fid : input_files) {
      auto pos = std::lower_bound(entry.tracked_phys_ids.begin(), 
                                  entry.tracked_phys_ids.end(), fid);
      if (pos != entry.tracked_phys_ids.end() && *pos == fid) {
          entry.tracked_phys_ids.erase(pos);
      }
  }

  // 2.2 添加生成的 SST 文件 
  // 只有当 Output File 有效 (非0) 时才添加
  if (output_file != 0) {
      auto pos = std::lower_bound(entry.tracked_phys_ids.begin(), 
                                  entry.tracked_phys_ids.end(), output_file);
      if (pos == entry.tracked_phys_ids.end() || *pos != output_file) {
          entry.tracked_phys_ids.insert(pos, output_file);
      }
  }

  // assert(entry.ref_count == (int32_t)entry.tracked_phys_ids.size());

  // 2.3 检查清理条件：无文件引用 且 标记为删除
  if (entry.ref_count <= 0 && entry.is_deleted) {
      shard.table.erase(it);
  }
}

void GlobalDeleteCountTable::ApplyFlushChange(uint64_t cuid,
                                              uint64_t output_file) {
  auto& shard = GetShard(cuid);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  if (it == shard.table.end()) return;

  auto& entry = it->second;
  auto pos = std::lower_bound(entry.tracked_phys_ids.begin(),
                              entry.tracked_phys_ids.end(), 0);
  if (pos != entry.tracked_phys_ids.end() && *pos == 0) {
    // 移除占位的 Memtable ID
    entry.tracked_phys_ids.erase(pos);
    auto out_pos = std::lower_bound(entry.tracked_phys_ids.begin(),
                                     entry.tracked_phys_ids.end(), output_file);
    if (out_pos == entry.tracked_phys_ids.end() || *out_pos != output_file) {
      entry.tracked_phys_ids.insert(out_pos, output_file);
      // ref_count 不修改
    } else {
      entry.ref_count--;
    }
  }
}

bool GlobalDeleteCountTable::MarkDeleted(uint64_t cuid) {
  auto& shard = GetShard(cuid);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  if (it != shard.table.end()) {
    it->second.is_deleted = true;
    return true; 
  }
  return false;
}

bool GlobalDeleteCountTable::IsDeleted(uint64_t cuid) const {
  const auto& shard = GetShard(cuid);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);  
  if (it != shard.table.end()) {
    return it->second.is_deleted;
  }
  return false;
}

int GlobalDeleteCountTable::GetRefCount(uint64_t cuid) const {
  const auto& shard = GetShard(cuid);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  auto it = shard.table.find(cuid);
  if (it != shard.table.end()) {
    return it->second.GetRefCount();
  }
  return 0;
}

bool GlobalDeleteCountTable::IsTracked(uint64_t cuid) const {
  const auto& shard = GetShard(cuid);
  std::shared_lock<std::shared_mutex> lock(shard.mutex);
  return shard.table.find(cuid) != shard.table.end();
}


} // namespace ROCKSDB_NAMESPACE