#pragma once
#include <cstdint>
#include "db/dbformat.h"
#include "rocksdb/slice.h"

// for delta
namespace ROCKSDB_NAMESPACE {

// L0层分区总数，默认为16
constexpr uint32_t kL0PartitionCount = 16;

/**
 * 从 User Key 中提取 Table ID
 * 预期 Key 的前缀布局为: [dbid:8B][tableid:8B][cuid:8B][key:...]
 * * @param user_key 输入的原始用户 Key
 * @param table_id 输出参数，用于存储解析出的 Table ID
 * @return 解析成功返回 true，格式不匹配或解析失败返回 false
 */
inline bool ExtractTableIdFromUserKey(const Slice& user_key, uint64_t* table_id) {
  constexpr size_t kDbIdBytes = 8;
  constexpr size_t kTableIdBytes = 8;
  constexpr size_t kMinPrefixBytes = kDbIdBytes + kTableIdBytes;
  if (user_key.size() < kMinPrefixBytes) {
    return false;
  }

  const char* data = user_key.data();
  // 按大端序读取 tableid（位于 user key 的 [8, 16) 字节）
  uint64_t value = 0;
  for (size_t i = 0; i < kTableIdBytes; ++i) {
    value = (value << 8) |
            static_cast<uint64_t>(static_cast<unsigned char>(data[kDbIdBytes + i]));
  }

  if (table_id != nullptr) {
    *table_id = value;
  }
  return true;
}

/**
 * 根据 User Key 计算对应的 L0 分区索引
 * 逻辑：解析出 Table ID，然后对分区总数取模
 * * @param user_key 用户 Key
 * @return 分区索引 (0 到 kL0PartitionCount-1)，若解析失败则默认返回 0
 */
inline uint32_t ExtractL0PartitionFromUserKey(const Slice& user_key) {
  uint64_t table_id = 0;
  if (!ExtractTableIdFromUserKey(user_key, &table_id)) {
    return 0;
  }
  return static_cast<uint32_t>(table_id % kL0PartitionCount);
}

/**
 * 根据 Internal Key 计算对应的 L0 分区索引
 * Internal Key 包含了 User Key 加上序列号和类型，此处需先提取 User Key
 */
inline uint32_t ExtractL0PartitionFromInternalKey(const Slice& internal_key) {
  // RocksDB 内部函数，用于剥离 Internal Key 末尾的 8 字节元数据
  return ExtractL0PartitionFromUserKey(ExtractUserKey(internal_key));
}

}  // namespace ROCKSDB_NAMESPACE
