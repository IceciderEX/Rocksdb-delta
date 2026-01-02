#include <iostream>
#include <string>
#include <vector>
#include <iomanip>
#include <sstream>
#include <thread>
#include <chrono>
#include <cstring>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"

using namespace ROCKSDB_NAMESPACE;

// ==========================================
// 1. Key 生成辅助函数 (适配 Delta 表结构)
// ==========================================
// 结构: dbid(8) + tableid(8) + cuid(8) + row_id_seq(...)
// 总前缀长度 24 字节，其中 CUID 位于偏移量 16 处 (Big Endian)
std::string GenerateKey(uint64_t cuid, int row_id) {
    std::string key;
    // 预分配足够空间: 16(prefix) + 8(cuid) + 20(row_suffix)
    key.resize(40); 
    
    // 1. 填充前 16 字节 (DBID + TableID) 为 0 (模拟固定值)
    std::memset(&key[0], 0, 16);

    // 2. 填充 CUID (Big Endian, 偏移量 16)
    unsigned char* p = reinterpret_cast<unsigned char*>(&key[0]) + 16;
    p[0] = (cuid >> 56) & 0xFF;
    p[1] = (cuid >> 48) & 0xFF;
    p[2] = (cuid >> 40) & 0xFF;
    p[3] = (cuid >> 32) & 0xFF;
    p[4] = (cuid >> 24) & 0xFF;
    p[5] = (cuid >> 16) & 0xFF;
    p[6] = (cuid >> 8)  & 0xFF;
    p[7] = (cuid >> 0)  & 0xFF;

    // 3. 填充 RowID (模拟后缀)
    // 注意：key 必须截断到正确长度，这里简单拼接字符串方便调试
    // 实际存储时 key.data() 前24字节是二进制，后面是字符串
    std::stringstream ss;
    ss << "_row_" << std::setw(6) << std::setfill('0') << row_id;
    std::string suffix = ss.str();
    
    // 将后缀拷贝到偏移量 24 之后
    std::memcpy(&key[0] + 24, suffix.data(), suffix.size());
    
    // 调整最终大小
    key.resize(24 + suffix.size());
    
    return key;
}

// 辅助函数：执行扫描并计数
int RunScan(DB* db, uint64_t target_cuid, const std::string& desc, bool print_sample = false) {
    std::cout << "\n--- " << desc << " (Target CUID: " << target_cuid << ") ---" << std::endl;
    
    ReadOptions read_options;
    std::unique_ptr<Iterator> it(db->NewIterator(read_options));
    
    // 构造 Start Key (Row 0)
    std::string start_key = GenerateKey(target_cuid, 0);
    
    int count = 0;
    // 这里的 Scan 逻辑是：Seek 到 CUID 开头，一直 Next 直到 CUID 变化
    // 为了简单，我们只扫特定的行数或直到 Iterator 无效
    for (it->Seek(start_key); it->Valid(); it->Next()) {
        // 简单判断：如果 key 长度不对或前缀不匹配(这里略过严格前缀检查，假设库是空的)
        if (it->key().size() < 24) break;
        
        // 解析 CUID 验证是否越界 (简单的 check)
        // 实际 ExtractCUID 逻辑在 RocksDB 内部，这里我们只看结果
        // 假设我们只写入了一个 CUID 的数据，所以 Valid() 为 false 就结束
        
        count++;
        if (print_sample && count <= 3) {
             std::cout << "   Sample [" << count << "]: " << it->key().ToString() 
                       << " => " << it->value().ToString() << std::endl;
        }
    }
    
    if (!it->status().ok()) {
        std::cerr << "   Iterator Error: " << it->status().ToString() << std::endl;
    }
    
    std::cout << "   Total rows scanned: " << count << std::endl;
    return count;
}

int main() {
    DB* db = nullptr;
    Options options;
    options.create_if_missing = true;
    
    // 设置一些可能影响 Flush 的参数，尽量保持默认以触发我们的逻辑
    // options.write_buffer_size = 64 << 20; 

    std::string dbname = "/home/wam/HWKV/rocksdb-delta/db_tmp";
    DestroyDB(dbname, options);

    Status s = DB::Open(options, dbname, &db);
    if (!s.ok()) {
        std::cerr << "Open DB failed: " << s.ToString() << std::endl;
        return 1;
    }

    // ==========================================
    // 场景参数
    // ==========================================
    uint64_t hot_cuid = 10086;      // 测试用的热点 CUID
    int num_rows = 2000;            // 写入行数
    int scan_threshold = 4;         // ScanFrequencyTable 的阈值 (假设是4)

    // ==========================================
    // Phase 1: Ingestion (实时入库)
    // ==========================================
    std::cout << ">>> Phase 1: Ingesting " << num_rows << " rows..." << std::endl;
    for (int i = 0; i < num_rows; i++) {
        std::string key = GenerateKey(hot_cuid, i);
        std::string value = "val_" + std::to_string(i); // 模拟较小的 payload
        s = db->Put(WriteOptions(), key, value);
        if (!s.ok()) {
            std::cerr << "Put failed: " << s.ToString() << std::endl;
            return 1;
        }
    }
    std::cout << ">>> Ingestion complete." << std::endl;

    // ==========================================
    // Phase 2: Scan-as-Compaction (触发热点识别与数据重组)
    // ==========================================
    // 我们需要多次 Scan 来让 FrequencyTable 标记该 CUID 为 Hot
    // 假设阈值是 4，我们扫 5 次
    std::cout << ">>> Phase 2: Scanning to trigger Hotspot Optimization..." << std::endl;
    
    for (int i = 1; i <= scan_threshold + 1; ++i) {
        std::string msg = "Scan Attempt " + std::to_string(i);
        // 执行全量扫描
        RunScan(db, hot_cuid, msg, (i == 1)); 
        
        // 只有最后一次 Scan (i > threshold) 理论上会触发 FlushGlobalBufferToSST
        // 你可以通过查看 stdout 中的 "[HotspotManager]" 日志来验证
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // ==========================================
    // Phase 3: Logical Delete (拦截删除)
    // ==========================================
    std::cout << ">>> Phase 3: Performing Logical Delete..." << std::endl;
    
    // 构造一个属于该 CUID 的任意 Key
    std::string delete_trigger_key = GenerateKey(hot_cuid, 0);
    
    // 调用 Delete。
    // 预期：InterceptDelete 会被调用，在 GlobalDeleteCountTable 中标记 CUID=10086 为 deleted。
    // RocksDB 底层可能不会写入 Tombstone (如果你的 InterceptDelete 返回了 true 并且上层处理了)
    s = db->Delete(WriteOptions(), delete_trigger_key);
    
    if (s.ok()) {
        std::cout << ">>> Delete operation successful (Logical Delete Triggered)." << std::endl;
    } else {
        std::cerr << ">>> Delete failed: " << s.ToString() << std::endl;
    }

    // ==========================================
    // Phase 4: Verification (验证数据是否不可见)
    // ==========================================
    std::cout << ">>> Phase 4: Verifying data visibility..." << std::endl;
    
    // 再次扫描
    // 预期：DBIter 检测到 CUID 被标记删除，应该跳过所有数据，返回行数为 0
    int visible_rows = RunScan(db, hot_cuid, "Post-Delete Scan");
    
    if (visible_rows == 0) {
        std::cout << ">>> SUCCESS: All data is hidden as expected!" << std::endl;
    } else {
        std::cout << ">>> FAILURE: Still see " << visible_rows << " rows. Logic not working." << std::endl;
    }

    delete db;
    return 0;
}