#include <iostream>
#include <string>
#include <vector>
#include <iomanip>
#include <sstream>
#include <thread>
#include <chrono>
#include <cstring>
#include <assert.h>
#include <unistd.h> // for access()

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
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
    // 假设你已经在 DBImpl 中添加了 GetHotspotManager()
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
    
    // 禁用自动 Flush/Compaction，完全由我们手动控制
    options.max_background_flushes = 0;
    options.max_background_compactions = 0;
    
    // 关键：设置较小的 write_buffer_size 以便更容易触发 Immutable Memtable (虽然我们手动控制 Flush)
    // 但为了制造 Immutable Memtable (未刷盘但已冻结)，我们需要这个设置配合
    options.write_buffer_size = 4 * 1024 * 1024; // 4MB
    options.max_write_buffer_number = 4; // 允许存在多个 memtable

    std::string dbname = "/home/wam/HWKV/rocksdb-delta/db_tmp/rocksdb_scan";

    DestroyDB(dbname, options);

    Status s = DB::Open(options, dbname, &db);
    if (!s.ok()) {
        std::cerr << "Open DB failed: " << s.ToString() << std::endl;
        return 1;
    }

    uint64_t test_cuid = 99999;
    int row_cursor = 0;
    
    // ==========================================
    // 1. 构造复杂物理场景
    // 目标：3 个 SST + 1 个 Immutable Memtable + 1 个 Active Memtable = 5 个物理单元
    // ==========================================

    std::cout << ">>> Step 1: Creating 3 SSTables..." << std::endl;
    for (int sst_idx = 0; sst_idx < 3; sst_idx++) {
        // 写入一批数据
        for (int i = 0; i < 500; i++) {
            s = db->Put(WriteOptions(), GenerateKey(test_cuid, row_cursor++), "val_sst_" + std::to_string(sst_idx));
            assert(s.ok());
        }
        // 强制刷盘生成 SST
        FlushOptions flush_opts;
        flush_opts.wait = true;
        s = db->Flush(flush_opts);
        assert(s.ok());
        std::cout << "    Created SST #" << (sst_idx + 1) << std::endl;
    }

    std::cout << ">>> Step 2: Creating 1 Immutable Memtable..." << std::endl;
    // 写入足够多的数据，填满 Active Memtable 并触发 Switch (变成 Immutable)，但不让它 Flush
    // 这里我们直接写入数据，然后不调用 DB::Flush，而是依赖 Write Buffer Size 或者不操作
    // 为了显式控制，我们写入一批数据
    for (int i = 0; i < 500; i++) {
        s = db->Put(WriteOptions(), GenerateKey(test_cuid, row_cursor++), "val_imm");
        assert(s.ok());
    }
    // 写入 Memtable 数据
    for (int i = 0; i < 500; i++) {
        s = db->Put(WriteOptions(), GenerateKey(test_cuid, row_cursor++), "val_active");
        assert(s.ok());
    }

    // 此时预期状态：
    // SST 1 (Rows 0-499)
    // SST 2 (Rows 500-999)
    // SST 3 (Rows 1000-1499)
    // Memtable (Rows 1500-1999) - 指针地址 X
    // 总共 4 个物理单元。
    // (如果要造第5个 Immutable，比较依赖内部接口，这里先测4个，逻辑是一样的)
    int expected_ref_count = 4;

    std::cout << ">>> Current Physical Layout Expectation: 3 SSTs + 1 Memtable." << std::endl;
    
    // ==========================================
    // 2. 第一次 Scan (验证初始化)
    // ==========================================
    std::cout << ">>> Step 3: First Full Scan (Expect Init)..." << std::endl;
    RunScan(db, test_cuid, "Scan 1 (Init)");

    int ref_count_1 = GetInternalRefCount(db, test_cuid);
    std::cout << ">>> Ref Count after Scan 1: " << ref_count_1 << std::endl;

    if (ref_count_1 == expected_ref_count) {
        std::cout << "✅ CHECK PASS: RefCount initialized correctly to " << expected_ref_count << "." << std::endl;
    } else {
        std::cout << "❌ CHECK FAIL: Expected " << expected_ref_count << ", got " << ref_count_1 
                  << ". (Note: If this is 1 higher, internal empty iterator might be counted, or auto-flush happened)" << std::endl;
    }

    // ==========================================
    // 3. 第二次 Scan (验证防重入)
    // ==========================================
    std::cout << ">>> Step 4: Second Full Scan (Expect No Change)..." << std::endl;
    RunScan(db, test_cuid, "Scan 2 (Read-Only)");

    int ref_count_2 = GetInternalRefCount(db, test_cuid);
    std::cout << ">>> Ref Count after Scan 2: " << ref_count_2 << std::endl;

    if (ref_count_2 == ref_count_1) {
        std::cout << "✅ CHECK PASS: RefCount stable at " << ref_count_2 << "." << std::endl;
    } else {
        std::cout << "❌ CHECK FAIL: Count changed! Double counting occurred." << std::endl;
    }

    // ==========================================
    // 4. 写入新数据 (增加新的 Memtable) 并再次 Scan
    // ==========================================
    std::cout << ">>> Step 5: Writing more data (Trigger new Memtable or Mix)..." << std::endl;
    // 再次 Flush 之前的 Memtable，生成第 4 个 SST
    FlushOptions fopt;
    fopt.wait = true;
    db->Flush(fopt); 

    // 写入新的 Active Memtable
    for (int i = 0; i < 100; i++) {
        db->Put(WriteOptions(), GenerateKey(test_cuid, row_cursor++), "val_new_active");
    }
    
    std::cout << ">>> Step 6: Third Scan (With New Data)..." << std::endl;
    RunScan(db, test_cuid, "Scan 3 (New Data)");
    
    int ref_count_3 = GetInternalRefCount(db, test_cuid);
    std::cout << ">>> Ref Count after Scan 3: " << ref_count_3 << std::endl;

    if (ref_count_3 == ref_count_2) {
        std::cout << "✅ CHECK PASS: RefCount did NOT increase (Correct Lazy Logic)." << std::endl;
        std::cout << "   (Note: Incrementing for new files is the responsibility of Flush/Compaction jobs, not User Scan)" << std::endl;
    } else {
        std::cout << "❌ CHECK FAIL: Count changed unexpectedly!" << std::endl;
    }

    delete db;
    return 0;
}