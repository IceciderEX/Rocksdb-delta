#include <iostream>
#include <string>
#include <vector>
#include <iomanip>
#include <sstream>
#include <thread>
#include <chrono>
#include <cstring>
#include <assert.h>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "db/db_impl/db_impl.h" 
#include "delta/hotspot_manager.h"
#include "delta/hot_index_table.h"

using namespace ROCKSDB_NAMESPACE;

// 数据库路径
const std::string kDBPath = "/home/wam/HWKV/rocksdb-delta/db_tmp";

// ==========================================
// Key 生成辅助函数
// ==========================================
std::string GenerateKey(uint64_t cuid, int row_id) {
    std::string key;
    key.resize(40); 
    std::memset(&key[0], 0, 40); 
    
    unsigned char* p = reinterpret_cast<unsigned char*>(&key[0]) + 16;
    p[0] = (cuid >> 56) & 0xFF;
    p[1] = (cuid >> 48) & 0xFF;
    p[2] = (cuid >> 40) & 0xFF;
    p[3] = (cuid >> 32) & 0xFF;
    p[4] = (cuid >> 24) & 0xFF;
    p[5] = (cuid >> 16) & 0xFF;
    p[6] = (cuid >> 8) & 0xFF;
    p[7] = (cuid) & 0xFF;

    std::string row_str = std::to_string(row_id);
    size_t copy_len = std::min(row_str.size(), key.size() - 24);
    std::memcpy(&key[24], row_str.data(), copy_len);

    return key;
}

void RunScan(DB* db, uint64_t target_cuid, const std::string& desc) {
    std::cout << "--- " << desc << " (Scanning CUID " << target_cuid << ") ---" << std::endl;
    ReadOptions ro;
    Iterator* it = db->NewIterator(ro);
    
    int count = 0;
    std::string start_key = GenerateKey(target_cuid, 0); 
    
    // 模拟 Range Scan: 扫到 key 不属于该 cuid 为止(Mock逻辑，实际DBIter会自动处理)
    // 这里简单起见，只扫固定数量，或者依赖 GenerateKey 的顺序性
    // 假设只写入了这几个 CUID，且它们 key 是聚簇的
    for (it->Seek(start_key); it->Valid(); it->Next()) {
        // 简单检查 CUID 前缀 (Mock Extract)
        // 实际测试中，如果 Scan 范围太大可能会扫到别人的 key，
        // 但我们的测试写入量小，且按 CUID 顺序写入，Seek 之后紧接着就是该 CUID 数据
        // 为了严谨，这里可以加个 check，如果不属于 target_cuid 就 break
        // 但 db_iter 内部已经处理了热点识别逻辑，这里只是为了触发 iter->Next
        count++;
        if (count >= 5000) break; // 限制一下扫描行数，模拟 Limit
    }
    
    if (!it->status().ok()) {
        std::cerr << "Scan error: " << it->status().ToString() << std::endl;
    }
    delete it;
    std::cout << "Scanned " << count << " rows (mock limit)." << std::endl;
}

int main() {
    // 1. 清理旧环境
    Options options;
    options.create_if_missing = true;
    Status s = DestroyDB(kDBPath, options);
    if (!s.ok()) {
        std::cerr << "DestroyDB failed: " << s.ToString() << std::endl;
    }

    DB* db = nullptr;
    s = DB::Open(options, kDBPath, &db);
    if (!s.ok()) {
        std::cerr << "Open DB failed: " << s.ToString() << std::endl;
        return 1;
    }

    DBImpl* db_impl = dynamic_cast<DBImpl*>(db);
    assert(db_impl != nullptr);
    auto hotspot_mgr = db_impl->GetHotspotManager();
    if (!hotspot_mgr) {
        std::cerr << "ERROR: HotspotManager is not initialized!" << std::endl;
        delete db;
        return 1;
    }
    
    // 定义三个 CUID：两个热，一个冷
    uint64_t cuid_hot_1 = 1001;
    uint64_t cuid_cold  = 2002;
    uint64_t cuid_hot_2 = 3003;
    
    int num_rows = 5000;

    // ==========================================
    // 2. 写入数据 (混合写入)
    // ==========================================
    std::cout << "\n>>> [Step 1] Writing data for 3 CUIDs..." << std::endl;
    WriteOptions wo;
    for (int i = 0; i < num_rows; i++) {
        db->Put(wo, GenerateKey(cuid_hot_1, i), "val_1_" + std::to_string(i));
        db->Put(wo, GenerateKey(cuid_cold,  i), "val_cold_" + std::to_string(i));
        db->Put(wo, GenerateKey(cuid_hot_2, i), "val_2_" + std::to_string(i));
    }
    
    // 强制 Flush 到 L0 SST
    FlushOptions fopt;
    fopt.wait = true;
    db->Flush(fopt); 
    std::cout << ">>> Flushed to L0." << std::endl;

    // ==========================================
    // 3. 预热阶段 (Warm up)
    // ==========================================
    std::cout << "\n>>> [Step 2] Warming up Hot CUIDs (Threshold = 4)..." << std::endl;
    
    // Hot 1: 扫描 4 次 -> 变热
    for (int i = 1; i <= 4; i++) RunScan(db, cuid_hot_1, "Warmup Hot1");
    
    // Hot 2: 扫描 4 次 -> 变热
    for (int i = 1; i <= 4; i++) RunScan(db, cuid_hot_2, "Warmup Hot2");

    // Cold: 扫描 1 次 -> 保持冷
    RunScan(db, cuid_cold, "Scan Cold (Once)");

    // ==========================================
    // 4. 触发 Scan-as-Compaction (Buffer 聚合测试)
    // ==========================================
    std::cout << "\n>>> [Step 3] Triggering SaC for Hot CUIDs..." << std::endl;
    
    // 第 5 次扫描 Hot 1：应该触发 ShouldTrigger -> 写入 Buffer
    RunScan(db, cuid_hot_1, "Trigger Hot1");
    
    // 第 5 次扫描 Hot 2：应该触发 ShouldTrigger -> 写入 Buffer
    // 注意：此时 Hot1 的数据还在 Buffer 里，Hot2 的数据会追加进去
    RunScan(db, cuid_hot_2, "Trigger Hot2");
    
    // 第 2 次扫描 Cold：冷数据，不应触发写入
    RunScan(db, cuid_cold, "Scan Cold (Twice)");

    // ==========================================
    // 5. 刷盘与验证
    // ==========================================
    std::cout << "\n>>> [Step 4] Triggering Shared Buffer Flush..." << std::endl;
    hotspot_mgr->TriggerBufferFlush();

    std::cout << "\n>>> [Step 5] Verifying Metadata Integrity..." << std::endl;
    
    HotIndexEntry entry1, entry2, entry_cold;
    
    // 验证 Hot 1
    bool has_hot1 = hotspot_mgr->GetIndexTable().GetEntry(cuid_hot_1, &entry1);
    if (has_hot1 && entry1.HasSnapshot()) {
        std::cout << "✅ Hot1 Found. FileNum: " << entry1.snapshot_segments[0].file_number 
                  << " Offset: " << entry1.snapshot_segments[0].offset << std::endl;
    } else {
        std::cout << "❌ Hot1 Failed!" << std::endl;
    }

    // 验证 Hot 2
    bool has_hot2 = hotspot_mgr->GetIndexTable().GetEntry(cuid_hot_2, &entry2);
    if (has_hot2 && entry2.HasSnapshot()) {
        std::cout << "✅ Hot2 Found. FileNum: " << entry2.snapshot_segments[0].file_number 
                  << " Offset: " << entry2.snapshot_segments[0].offset << std::endl;
    } else {
        std::cout << "❌ Hot2 Failed!" << std::endl;
    }

    // 验证 Cold
    bool has_cold = hotspot_mgr->GetIndexTable().GetEntry(cuid_cold, &entry_cold);
    if (!has_cold || !entry_cold.HasSnapshot()) {
        std::cout << "✅ Cold CUID correctly ignored (No Snapshot)." << std::endl;
    } else {
        std::cout << "❌ Cold CUID should not have snapshot!" << std::endl;
    }

    // ==========================================
    // 6. 验证共享存储 (Shared SST Check)
    // ==========================================
    std::cout << "\n>>> [Step 6] Verifying Shared Storage..." << std::endl;
    if (has_hot1 && has_hot2) {
        uint64_t file1 = entry1.snapshot_segments[0].file_number;
        uint64_t file2 = entry2.snapshot_segments[0].file_number;
        
        if (file1 == file2) {
            std::cout << "✅ SUCCESS: Both CUIDs share the same SST file (ID: " << file1 << ")" << std::endl;
            
            // 验证 Offset 不重叠
            uint64_t off1 = entry1.snapshot_segments[0].offset;
            uint64_t len1 = entry1.snapshot_segments[0].length;
            uint64_t off2 = entry2.snapshot_segments[0].offset;
            
            std::cout << "   CUID1 Range: [" << off1 << ", " << off1 + len1 << ")" << std::endl;
            std::cout << "   CUID2 Range: [" << off2 << ", ...)" << std::endl;

            if (off1 + len1 <= off2 || off2 + entry2.snapshot_segments[0].length <= off1) {
                std::cout << "✅ SUCCESS: Data segments do not overlap." << std::endl;
            } else {
                std::cout << "❌ FAILURE: Data segments overlap!" << std::endl;
            }
        } else {
            std::cout << "⚠️ NOTICE: CUIDs are in different files. (Did Flush trigger twice? Or buffer rotated?)" << std::endl;
        }
    }

    delete db;
    std::cout << "\nTest Finished." << std::endl;
    return 0;
}