#pragma once

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

inline uint64_t ExtractCUID(const Slice& key) {
  if (key.size() < 24) return 0;
  const unsigned char* p =
      reinterpret_cast<const unsigned char*>(key.data()) + 16;
  uint64_t c = 0;
  for (int i = 0; i < 8; ++i) {
    c = (c << 8) | p[i];
  }
  return c;
}

}  // namespace ROCKSDB_NAMESPACE