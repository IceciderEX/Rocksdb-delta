#include <iostream>
#include <string>
#include <iomanip>
#include <sstream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"

using namespace ROCKSDB_NAMESPACE;

// 构造 key：key_000 ~ key_009
std::string Key(int i) {
    std::stringstream ss;
    ss << "key_" << std::setw(3) << std::setfill('0') << i;
    return ss.str();
}

int main() {
    DB* db = nullptr;
    Options options;
    options.create_if_missing = true;

    std::string dbname = "/home/wam/HWKV/rocksdb-delta/db_tmp/rocksdb_scan";

    DestroyDB(dbname, options);

    Status s = DB::Open(options, dbname, &db);
    if (!s.ok()) {
        std::cerr << "Open DB failed: " << s.ToString() << std::endl;
        return 1;
    }

    for (int i = 0; i < 10; i++) {
        std::string key = Key(i);
        std::string value = "value_" + std::to_string(i);
        s = db->Put(WriteOptions(), key, value);
        if (!s.ok()) {
            std::cerr << "Put failed: " << s.ToString() << std::endl;
            return 1;
        }
    }

    std::cout << "=== Scan Start ===" << std::endl;

    ReadOptions read_options;
    std::unique_ptr<Iterator> it(db->NewIterator(read_options));

    // 从 key_003 开始 scan
    std::string start_key = Key(3);
    std::string end_key   = Key(8);

    for (it->Seek(start_key);
         it->Valid() && it->key().ToString() < end_key;
         it->Next()) {

        std::cout << it->key().ToString()
                  << " => "
                  << it->value().ToString()
                  << std::endl;
    }

    if (!it->status().ok()) {
        std::cerr << "Iterator error: " << it->status().ToString() << std::endl;
    }

    std::cout << "=== Scan End ===" << std::endl;

    it.reset();
    delete db;
    return 0;
}
