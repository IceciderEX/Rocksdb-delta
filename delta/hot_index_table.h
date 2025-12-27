#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <optional>

namespace ROCKSDB_NAMESPACE {

struct HotspotEntry {
  std::string snapshot_file_path;

  // std::vector<...> deltas; 

  bool IsValid() const {
    return !snapshot_file_path.empty();
  }
};

class HotIndexTable {
 public:
  HotIndexTable() = default;

  // 更新某个 CUID 的快照文件路径
  void UpdateSnapshot(uint64_t cuid, const std::string& path);

  // 查询 CUID 是否有热点数据文件
  bool GetEntry(uint64_t cuid, HotspotEntry* entry) const;

  // 移除某个 CUID 的索引（例如数据过期或被彻底删除）
  void RemoveEntry(uint64_t cuid);

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<uint64_t, HotspotEntry> table_;
};

}  // namespace ROCKSDB_NAMESPACE