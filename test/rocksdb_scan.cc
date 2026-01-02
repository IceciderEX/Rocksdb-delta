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
// 为了验证内部计数，我们需要访问 DBImpl
#include "db/db_impl/db_impl.h" 
#include "delta/hotspot_manager.h"

using namespace ROCKSDB_NAMESPACE;

// ==========================================
// Key 生成辅助函数
// ==========================================
std::string GenerateKey(uint64_t cuid, int row_id) {
    std::string key;
    key.resize(40); 
    std::memset(&key[0], 0, 16);
    unsigned char* p = reinterpret_cast<unsigned char*>(&key[0]) + 16;
    p[0] = (cuid >> 56) & 0xFF;
    p[1] = (cuid >> 48) & 0xFF;
    p[2] = (cuid >> 40) & 0xFF;
    p[3] = (cuid >> 32) & 0xFF;
    p[4] = (cuid >> 24) & 0xFF;
    p[5] = (cuid >> 16) & 0xFF;
    p[6] = (cuid >> 8)  & 0xFF;
    p[7] = (cuid >> 0)  & 0xFF;

    std::stringstream ss;
    ss << "_row_" << std::setw(6) << std::setfill('0') << row_id;
    std::string suffix = ss.str();
    std::memcpy(&key[0] + 24, suffix.data(), suffix.size());
    key.resize(24 + suffix.size());
    return key;
}

int GetInternalRefCount(DB* db, uint64_t cuid) {
    auto db_impl = static_cast<DBImpl*>(db);
    return db_impl->GetHotspotManager()->GetDeleteTable().GetRefCount(cuid);
}

// 辅助函数：执行扫描
void RunScan(DB* db, uint64_t target_cuid, const std::string& desc) {
    std::cout << "\n--- " << desc << " ---" << std::endl;
    ReadOptions read_options;
    std::unique_ptr<Iterator> it(db->NewIterator(read_options));
    std::string start_key = GenerateKey(target_cuid, 0);
    int count = 0;
    for (it->Seek(start_key); it->Valid(); it->Next()) {
        if (it->key().size() < 24) break;
        count++;
    }
    if (!it->status().ok()) {
        std::cerr << "Iterator Error: " << it->status().ToString() << std::endl;
    }
    std::cout << "   Scanned Rows: " << count << std::endl;
}

int main() {
    DB* db = nullptr;
    Options options;
    options.create_if_missing = true;
    // 禁用自动 Flush/Compaction，完全由我们手动控制，以便精确测试计数
    options.max_background_flushes = 0;
    options.max_background_compactions = 0;

    std::string dbname = "/home/wam/HWKV/rocksdb-delta/db_tmp/rocksdb_scan";
    DestroyDB(dbname, options);

    Status s = DB::Open(options, dbname, &db);
    if (!s.ok()) {
        std::cerr << "Open DB failed: " << s.ToString() << std::endl;
        return 1;
    }

    uint64_t test_cuid = 99999;
    
    // ==========================================
    // 1. 构造混合场景 (SST + Memtable)
    // ==========================================
    std::cout << ">>> Step 1: Writing Batch 1 (Will be Flushed)..." << std::endl;
    // 写入 1000 行
    for (int i = 0; i < 1000; i++) {
        s = db->Put(WriteOptions(), GenerateKey(test_cuid, i), "val_old");
        assert(s.ok());
    }

    // 强制 Flush，生成第 1 个 SST 文件
    std::cout << ">>> Step 2: Flushing Batch 1 to SST..." << std::endl;
    FlushOptions flush_opts;
    flush_opts.wait = true;
    s = db->Flush(flush_opts);
    assert(s.ok());

    std::cout << ">>> Step 3: Writing Batch 2 (Stays in Memtable)..." << std::endl;
    // 写入另外 1000 行（row_id 接着上面，避免覆盖，虽然覆盖也算同一 Memtable）
    for (int i = 1000; i < 2000; i++) {
        s = db->Put(WriteOptions(), GenerateKey(test_cuid, i), "val_new");
        assert(s.ok());
    }

    // 此时物理状态预期：
    // 1. SSTable x 1 (包含 row 0-999)
    // 2. Memtable x 1 (包含 row 1000-1999)
    // CUID 99999 分布在 2 个物理单元中。
    
    // ==========================================
    // 2. 第一次 Scan (验证初始化)
    // ==========================================
    std::cout << ">>> Step 4: First Full Scan (Expect Init)..." << std::endl;
    RunScan(db, test_cuid, "Scan 1");

    int ref_count_1 = GetInternalRefCount(db, test_cuid);
    std::cout << ">>> Ref Count after Scan 1: " << ref_count_1 << std::endl;

    // 验证逻辑：应该正好是 2 (1 SST + 1 Memtable)
    // 注意：如果还有 immutable memtable 未刷盘，可能是 3，但在我们的设定下应为 2
    if (ref_count_1 == 2) {
        std::cout << "✅ CHECK PASS: RefCount initialized correctly to 2." << std::endl;
    } else {
        std::cout << "❌ CHECK FAIL: Expected 2, got " << ref_count_1 << ". (Check counting logic)" << std::endl;
    }

    // ==========================================
    // 3. 第二次 Scan (验证防重入)
    // ==========================================
    std::cout << ">>> Step 5: Second Full Scan (Expect No Change)..." << std::endl;
    RunScan(db, test_cuid, "Scan 2");

    int ref_count_2 = GetInternalRefCount(db, test_cuid);
    std::cout << ">>> Ref Count after Scan 2: " << ref_count_2 << std::endl;

    if (ref_count_2 == ref_count_1) {
        std::cout << "✅ CHECK PASS: RefCount stable (Lazy Init logic works)." << std::endl;
    } else {
        std::cout << "❌ CHECK FAIL: Count changed! Double counting occurred." << std::endl;
    }

    delete db;
    return 0;
}