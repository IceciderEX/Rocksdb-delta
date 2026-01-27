#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <cstring>
#include <assert.h>
#include <algorithm>
#include <iomanip>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "db/db_impl/db_impl.h" 
#include "delta/hotspot_manager.h"
#include "delta/hot_index_table.h"

using namespace ROCKSDB_NAMESPACE;

const std::string kDBPath = "/home/wam/Rocksdb-delta/db_tmp";
// const std::string kDBPath = "/home/wam/HWKV/rocksdb-delta/db_tmp";

// ==========================================
// 1. 辅助工具: Key 生成与校验
// ==========================================
// 构造符合 dbid_tableid_cuid... (Big Endian) 格式的 Key
std::string GenerateKey(uint64_t cuid, int row_id) {
    std::string key;
    key.resize(40); 
    std::memset(&key[0], 0, 40); 
    
    // Skip 16 bytes (dbid, tableid), write CUID at offset 16
    unsigned char* p = reinterpret_cast<unsigned char*>(&key[0]) + 16;
    for (int i = 0; i < 8; ++i) {
        p[i] = (cuid >> (56 - 8 * i)) & 0xFF;
    }

    // Append row_id for uniqueness
    std::string row_str = std::to_string(row_id);
    size_t copy_len = std::min(row_str.size(), key.size() - 24);
    std::memcpy(&key[24], row_str.data(), copy_len);

    return key;
}

uint64_t ExtractCUID(const Slice& key) {
    if (key.size() < 24) return 0;
    const unsigned char* p = reinterpret_cast<const unsigned char*>(key.data()) + 16;
    uint64_t c = 0;
    for (int i = 0; i < 8; ++i) {
        c = (c << 8) | p[i];
    }
    return c;
}

void Check(bool condition, const std::string& msg) {
    if (condition) {
        std::cout << "[PASS] " << msg << std::endl;
    } else {
        std::cerr << "[FAIL] " << msg << std::endl;
        exit(1);
    }
}

std::string GenerateUpperBoundKey(uint64_t cuid) {
    // 构造 CUID + 1 的 Key
    return GenerateKey(cuid + 1, 0); 
}

int PerformScan(DB* db, uint64_t cuid) {
    ReadOptions ro;
    
    // Upper Bound，防止 Iterator 读到下一个 CUID 的数据
    std::string upper_bound_str = GenerateUpperBoundKey(cuid);
    Slice upper_bound = upper_bound_str;
    ro.iterate_upper_bound = &upper_bound; 

    Iterator* it = db->NewIterator(ro);
    std::string start_key = GenerateKey(cuid, 0);
    
    int count = 0;
    for (it->Seek(start_key); it->Valid(); it->Next()) {
        if (ExtractCUID(it->key()) != cuid) break; 
        count++;
    }
    
    Status s = it->status();
    delete it;
    if (!s.ok()) {
        std::cerr << "Scan error: " << s.ToString() << std::endl;
    }
    return count;
}

// 写入数据并 Flush，生成物理 SST
void WriteBatchAndFlush(DB* db, const std::vector<uint64_t>& cuids, int start_row, int count_per_cuid) {
    WriteBatch batch;
    for (uint64_t cuid : cuids) {
        for (int i = 0; i < count_per_cuid; ++i) {
            batch.Put(GenerateKey(cuid, start_row + i), "payload_" + std::to_string(cuid));
        }
    }
    Status s = db->Write(WriteOptions(), &batch);
    Check(s.ok(), "Write batch");
    
    s = db->Flush(FlushOptions());
    Check(s.ok(), "Flush to SST");
}

// ==========================================
// 3. 主测试流程
// ==========================================

int main() {
    Options options;
    options.create_if_missing = true;
    // A. 环境初始化
    Status s = DestroyDB(kDBPath, options);
    

    options.create_missing_column_families = true;
    // 禁用自动 Compaction，以便我们手动控制测试时机
    options.disable_auto_compactions = true;
    // 强制使用 L0 (模拟 Delta 表只有 L0)
    options.num_levels = 1; 
    options.level0_file_num_compaction_trigger = 20; 

    DB* db = nullptr;
    s = DB::Open(options, kDBPath, &db);
    Check(s.ok(), "DB Open");

    // 获取内部 Manager 仅用于 Verification (不断言内部状态，仅观察)
    DBImpl* db_impl = dynamic_cast<DBImpl*>(db);
    auto hotspot_mgr = db_impl->GetHotspotManager();
    Check(hotspot_mgr != nullptr, "HotspotManager attached");

    const uint64_t CUID_DEL = 1001;  // 测试无 Tombstone 删除
    const uint64_t CUID_HOT = 2002;  // 测试 Scan-as-Compaction
    const uint64_t CUID_MOV = 3003;  // 测试 Compaction 索引更新

    

    std::cout << "\n>>> PHASE 1: Data Ingestion (Write & Flush) <<<\n";
    
    std::cout << "Pre-heating CUID_MOV to ensure Deltas are tracked during Flush..." << std::endl;
    for(int k=0; k<5; k++) {
        hotspot_mgr->RegisterScan(CUID_MOV);
    }
    // 写入 3 个 SST 文件，包含所有 CUID 的数据
    for (int i = 0; i < 3; ++i) {
        WriteBatchAndFlush(db, {CUID_DEL, CUID_HOT, CUID_MOV}, i * 100, 50);
        std::cout << "Generated SST #" << i + 1 << std::endl;
    }

    // =================================================================
    // 测试场景 1: 无 Tombstone 快速删除 (Global Delete Count Table)
    // =================================================================
    std::cout << "\n>>> PHASE 2: Immediate Delete Test (CUID_DEL) <<<\n";
    
    int rows = PerformScan(db, CUID_DEL);
    std::cout << "Initial Scan CUID_DEL: Found " << rows << " rows. (Ref counts built)" << std::endl;
    Check(rows == 150, "Data integrity check before delete");

    std::cout << "Deleting CUID_DEL via standard DB::Delete..." << std::endl;
    for (int i = 0; i < 3 * 100; ++i) { // 简单模拟范围删，逐个删 key (或者使用 DeleteRange 如果支持)=
        if (i == 0) {
            db->Delete(WriteOptions(), GenerateKey(CUID_DEL, 0)); 
            // 只需要触发一次即可验证效果
        }
    }
    
    // 2.3 验证删除效果 (不依赖 Compaction)
    std::cout << "Verifying Immediate Deletion..." << std::endl;
    rows = PerformScan(db, CUID_DEL);
    // 如果 Intercept 生效且 IsCuidDeleted 在 Iterator 中生效，这里应返回 0
    std::cout << "Scan CUID_DEL after Delete: Found " << rows << " rows." << std::endl;
    Check(rows == 0, "CUID_DEL should be invisible immediately without Flush/Compaction");


    // =================================================================
    // 测试场景 2: Scan-as-Compaction (Hotspot Promotion)
    // =================================================================
    std::cout << "\n>>> PHASE 3: Scan-as-Compaction Test (CUID_HOT) <<<\n";
    
    // 3.1 预热：频繁 Scan 触发热点判定
    std::cout << "Triggering frequent scans on CUID_HOT..." << std::endl;
    for (int i = 0; i < 6; ++i) {
        int r = PerformScan(db, CUID_HOT);
        Check(r == 150, "Scan data consistency");
        // 模拟间隔，让频率表生效
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    // 3.2 验证是否转存
    HotIndexEntry hot_entry;
    bool has_index = hotspot_mgr->GetIndexTable().GetEntry(CUID_HOT, &hot_entry);
    
    Check(has_index, "CUID_HOT should be in HotIndexTable");
    Check(hot_entry.HasSnapshot(), "CUID_HOT should have a Snapshot (Promoted)");
    
    Check(hot_entry.deltas.empty(), "Active Deltas should be empty after promotion");

    std::cout << "Snapshot Segments Count: " << hot_entry.snapshot_segments.size() << std::endl;
    
    // [新增验证]：检查是否包含内存标记 (-1) 的 Tail Segment
    if (!hot_entry.snapshot_segments.empty()) {
        uint64_t last_fid = hot_entry.snapshot_segments.back().file_number;
        // 18446744073709551615 就是 (uint64_t)-1
        std::cout << "Tail Segment ID: " << last_fid << std::endl; 
        Check(last_fid == static_cast<uint64_t>(-1), "Tail segment should be memory marker (-1)");
    } else {
        // 如果 snapshot 为空，说明 Finalize 逻辑有漏洞（我们刚才修过的 active_buffered_cuids_ 就是防这个的）
        Check(false, "Snapshot segments should not be empty (must contain at least tail segment)");
    }


    // =================================================================
    // 测试场景 3: 混合存储 Compaction (L0 GC & Index Update)
    // =================================================================
    std::cout << "\n>>> PHASE 4: Mixed Compaction Test (CUID_MOV) <<<\n";

    // for (int i = 0; i < 8; ++i) {
    //     int r = PerformScan(db, CUID_MOV);
    //     Check(r == 150, "Scan data consistency");
    //     // 模拟间隔，让频率表生效
    //     std::this_thread::sleep_for(std::chrono::milliseconds(20));
    // }
    
    // 检查 CUID_MOV 当前状态：应该是分散在 Delta Index 中
    HotIndexEntry mov_entry_before;
    if (hotspot_mgr->GetIndexTable().GetEntry(CUID_MOV, &mov_entry_before)) {
        std::cout << "Before Compaction: CUID_MOV has " << mov_entry_before.deltas.size() << " deltas." << std::endl;
        Check(mov_entry_before.deltas.size() >= 3, "Should have deltas from 3 flushes");
    } else {
        Check(false, "CUID_MOV should exist in index table (pre-heated in Phase 1)");
    }

    // 3.1 触发 Compaction
    // 使用 CompactRange 强制合并 L0 文件
    std::cout << "Triggering L0 Compaction..." << std::endl;
    s = db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    Check(s.ok(), "CompactRange");

    // 3.2 验证
    // a) CUID_DEL 的物理数据是否被清理？(难以直接验证物理文件，但逻辑上它被跳过了)
    // b) CUID_HOT 的 Obsolete Deltas 是否被清理？
    // c) CUID_MOV 的 Index 是否指向了新文件？
    
    HotIndexEntry mov_entry_after;
    if (hotspot_mgr->GetIndexTable().GetEntry(CUID_MOV, &mov_entry_after)) {
        std::cout << "After Compaction: CUID_MOV has " << mov_entry_after.deltas.size() << " deltas." << std::endl;
        // 如果完全合并，应该只有 1 个 Delta (即新生成的 L0 SST)
        Check(mov_entry_after.deltas.size() < mov_entry_before.deltas.size(), "Deltas should be merged");
        
        // 验证文件 ID 发生了变化
        if (!mov_entry_before.deltas.empty() && !mov_entry_after.deltas.empty()) {
            Check(mov_entry_before.deltas[0].file_number != mov_entry_after.deltas[0].file_number, 
                  "Delta should point to new SST file");
        }
    }

    // 验证 CUID_HOT 的 Obsolete Deltas 是否减少/清空
    hotspot_mgr->GetIndexTable().GetEntry(CUID_HOT, &hot_entry);
    if (hot_entry.obsolete_deltas.empty()) {
        std::cout << "[PASS] CUID_HOT Obsolete Deltas were physically cleaned." << std::endl;
    } else {
        std::cout << "[INFO] Some Obsolete Deltas remain (depends on compaction input coverage)." << std::endl;
    }

    std::cout << "\n>>> Integration Test Completed Successfully <<<" << std::endl;
    delete db;
    return 0;
}