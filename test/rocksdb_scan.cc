#include <iostream>
#include <string>
#include <vector>
#include <iomanip>
#include <sstream>
#include <thread>
#include <chrono>
#include <cstring>
#include <assert.h>
#include <algorithm>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "db/db_impl/db_impl.h" 
#include "delta/hotspot_manager.h"
#include "delta/hot_index_table.h"
#include "delta/global_delete_count_table.h"

using namespace ROCKSDB_NAMESPACE;

const std::string kDBPath = "/home/wam/HWKV/rocksdb-delta/db_test_l0";

// ==========================================
// Key 生成辅助函数 (与之前一致)
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

// 辅助：获取文件列表
std::vector<LiveFileMetaData> GetLiveFiles(DB* db) {
    std::vector<LiveFileMetaData> files;
    db->GetLiveFilesMetaData(&files);
    return files;
}

int main() {
    // 1. 初始化环境
    Options options;
    options.create_if_missing = true;
    options.level0_file_num_compaction_trigger = 20; // 配合我们修改后的 Picker
    options.disable_auto_compactions = true; // 先禁用自动，手动控制 Flush 积累文件
    
    Status s = DestroyDB(kDBPath, options);
    if (!s.ok()) std::cerr << "DestroyDB error: " << s.ToString() << std::endl;

    DB* db = nullptr;
    s = DB::Open(options, kDBPath, &db);
    if (!s.ok()) {
        std::cerr << "Open DB failed: " << s.ToString() << std::endl;
        return 1;
    }

    DBImpl* db_impl = dynamic_cast<DBImpl*>(db);
    auto hotspot_mgr = db_impl->GetHotspotManager();
    if (!hotspot_mgr) {
        std::cerr << "HotspotManager not initialized!" << std::endl;
        return 1;
    }

    // 定义三个测试对象
    uint64_t CUID_DELETE = 1001; // 将被全局删除
    uint64_t CUID_HOT    = 2002; // 将被热点分离 (Obsolete)
    uint64_t CUID_NORMAL = 3003; // 正常合并

    // ==========================================
    // 2. 生成 L0 文件 (积累 20 个以触发 L0Compaction)
    // ==========================================
    std::cout << "\n>>> [Step 1] Generating 20 L0 files (Trigger Threshold)..." << std::endl;
    
    int files_count = 20;
    int rows_per_file = 10;

    for (int i = 0; i < files_count; i++) {
        for (int r = 0; r < rows_per_file; r++) {
            // 每个文件都包含这三个 CUID 的数据
            db->Put(WriteOptions(), GenerateKey(CUID_DELETE, i*100+r), "del_val");
            db->Put(WriteOptions(), GenerateKey(CUID_HOT,    i*100+r), "hot_val");
            db->Put(WriteOptions(), GenerateKey(CUID_NORMAL, i*100+r), "norm_val");
        }
        // 每次写入后强制 Flush，生成一个 SST
        FlushOptions fopt;
        fopt.wait = true;
        db->Flush(fopt);
    }

    auto files = GetLiveFiles(db);
    std::cout << ">>> Generated " << files.size() << " L0 files." << std::endl;
    if (files.size() < 20) {
        std::cerr << "⚠️ Warning: Not enough files to trigger L0Compaction (Need >= 20)" << std::endl;
    }

    // ==========================================
    // 3. 构建元数据状态 (模拟 Flush Hook 和 Scan-as-Compaction)
    // ==========================================
    std::cout << "\n>>> [Step 2] Setting up Metadata..." << std::endl;

    // 3.1 模拟 Flush 后的 Index 注册
    // 由于我们还没有实现 FlushJob 的 Hook，这里手动把这 20 个文件注册到 HotIndexTable
    // 告诉 Manager：CUID_HOT 和 CUID_NORMAL 在这些文件里有 Delta
    for (const auto& f : files) {
        DataSegment seg;
        seg.file_number = f.file_number;
        seg.offset = 0; 
        seg.length = f.size; // 粗略估计
        
        // 注册 HOT 的 Delta
        hotspot_mgr->GetIndexTable().AddDelta(CUID_HOT, seg);
        // 注册 NORMAL 的 Delta
        hotspot_mgr->GetIndexTable().AddDelta(CUID_NORMAL, seg);
    }

    // 3.2 设置全局删除 (CUID_DELETE)
    std::cout << "    Marking CUID " << CUID_DELETE << " as DELETED." << std::endl;
    hotspot_mgr->GetDeleteTable().MarkDeleted(CUID_DELETE);

    // 3.3 设置热点 Obsolete (CUID_HOT)
    std::cout << "    Marking CUID " << CUID_HOT << " as HOT (Simulating Scan-as-Compaction)." << std::endl;
    // 模拟 Scan-as-Compaction 完成：
    // 1. 注册为热点
    hotspot_mgr->RegisterScan(CUID_HOT); 
    // 2. 创建一个假的 Snapshot (表示数据已搬迁)
    DataSegment snap_seg;
    snap_seg.file_number = 99999; // 假文件
    std::vector<DataSegment> new_snaps = {snap_seg};
    // 3. UpdateSnapshot 会自动把上面注册的 Deltas 移动到 Obsolete 列表
    hotspot_mgr->GetIndexTable().UpdateSnapshot(CUID_HOT, new_snaps);

    // ==========================================
    // 4. 触发 L0 Compaction
    // ==========================================
    std::cout << "\n>>> [Step 3] Triggering L0 Compaction..." << std::endl;
    
    // 开启自动 Compaction 并手动触发一次
    s = db->EnableAutoCompaction({db->DefaultColumnFamily()});
    // 使用 CompactRange 强行触发，我们的 Picker 会拦截并执行 L0->L0
    s = db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    
    if (!s.ok()) {
        std::cerr << "CompactRange failed: " << s.ToString() << std::endl;
    } else {
        std::cout << ">>> Compaction finished." << std::endl;
    }

    // ==========================================
    // 5. 验证结果
    // ==========================================
    std::cout << "\n>>> [Step 4] Verifying Results..." << std::endl;

    auto VerifyData = [&](uint64_t cuid, const std::string& name, bool should_exist) {
        std::cout << "    Verifying " << name << " (" << cuid << ")... ";
        int found_count = 0;
        ReadOptions ro;
        Iterator* it = db->NewIterator(ro);
        std::string start = GenerateKey(cuid, 0);
        for (it->Seek(start); it->Valid(); it->Next()) {
            uint64_t k_cuid = hotspot_mgr->ExtractCUID(it->key());
            if (k_cuid != cuid) break;
            found_count++;
        }
        delete it;

        if (should_exist) {
            if (found_count > 0) std::cout << "✅ OK (Found " << found_count << " rows)" << std::endl;
            else std::cout << "❌ FAIL (Expected data, but found none)" << std::endl;
        } else {
            if (found_count == 0) std::cout << "✅ OK (Data cleaned)" << std::endl;
            else std::cout << "❌ FAIL (Expected 0 rows, found " << found_count << ")" << std::endl;
        }
    };

    // 验证 1: CUID_DELETE 应该被物理删除了
    VerifyData(CUID_DELETE, "CUID_DELETE", false);

    // 验证 2: CUID_HOT 应该被物理删除了 (因为 Index 里标记为 Obsolete)
    VerifyData(CUID_HOT, "CUID_HOT", false);

    // 验证 3: CUID_NORMAL 应该还在，且文件被合并了
    VerifyData(CUID_NORMAL, "CUID_NORMAL", true);

    // 验证 4: CUID_NORMAL 的 Index 是否更新
    HotIndexEntry entry_norm;
    if (hotspot_mgr->GetIndexTable().GetEntry(CUID_NORMAL, &entry_norm)) {
        if (!entry_norm.deltas.empty()) {
            uint64_t new_file_num = entry_norm.deltas[0].file_number;
            auto new_files = GetLiveFiles(db);
            bool file_exists_on_disk = false;
            for(auto& f : new_files) {
                if (f.file_number == new_file_num) file_exists_on_disk = true;
            }
            
            std::cout << "    Verifying CUID_NORMAL Index... ";
            if (file_exists_on_disk) {
                std::cout << "✅ OK (Updated to new file " << new_file_num << ")" << std::endl;
            } else {
                std::cout << "❌ FAIL (Index points to non-existent file " << new_file_num << ")" << std::endl;
            }
        } else {
            std::cout << "❌ FAIL (CUID_NORMAL has no Deltas)" << std::endl;
        }
    }

    delete db;
    return 0;
}