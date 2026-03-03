/**
 * test/delta_component_test.cc
 * 深入测试 Delta Table 的各个独立子组件（GDCT, HotspotManager 等）寻找边角问题
 */

#include <iostream>
#include <cassert>
#include <vector>

#include "delta/global_delete_count_table.h"
#include "delta/hotspot_manager.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;

void Check(bool condition, const std::string& msg) {
  if (condition) {
    std::cout << "[PASS] " << msg << std::endl;
  } else {
    std::cerr << "[FAIL] " << msg << std::endl;
    exit(1);
  }
}

void TestGDCT() {
  std::cout << "--- Testing GlobalDeleteCountTable ---" << std::endl;
  GlobalDeleteCountTable gdct;
  uint64_t cuid = 1001;

  // 1. 初始记录引用
  bool new_track = gdct.TrackPhysicalUnit(cuid, 10);
  Check(new_track == true, "首次追踪应返回 true");
  Check(gdct.GetRefCount(cuid) == 1, "引用计数应为 1");

  // 2. 重复记录相同物理 ID
  new_track = gdct.TrackPhysicalUnit(cuid, 10);
  Check(new_track == false, "追踪相同 phys_id 不应增加并返回 false");
  Check(gdct.GetRefCount(cuid) == 1, "引用计数仍应为 1");

  // 3. 记录新物理 ID
  new_track = gdct.TrackPhysicalUnit(cuid, 11);
  Check(new_track == true, "追踪新 phys_id 应返回 true");
  Check(gdct.GetRefCount(cuid) == 2, "引用计数应变为 2");

  // 4. 测试删除标记
  bool marked = gdct.MarkDeleted(cuid);
  Check(marked == true, "标记删除应成功");
  Check(gdct.IsDeleted(cuid) == true, "IsDeleted 应返回 true");


  // 6. 批量 Untrack 逻辑
  std::vector<uint64_t> to_untrack = {10, 999}; // 999 并不在集合中，测试容错性
  gdct.UntrackFiles(cuid, to_untrack);
  Check(gdct.GetRefCount(cuid) == 1, "Untrack 后引用计数应为 1");

  std::cout << "GDCT 逻辑表现正常." << std::endl;
}

void TestHotspotManagerMergeQueue() {
  std::cout << "\n--- Testing HotspotManager Merge Queue ---" << std::endl;
  Options db_options;
  HotspotManager hotspot(db_options, "/home/wam/Rocksdb-delta/db_tmp2/hotspot_test_dir");

  uint64_t cuid = 2002;
  std::string start_key = "test_start";
  std::string end_key = "test_end";

  // 1. 初始化队列为空
  Check(!hotspot.HasPendingPartialMerge(), "初始队列应为空");

  // 2. 将同一个任务 enqueue 两次，测试防重入/防重复去重逻辑
  hotspot.EnqueuePartialMerge(cuid, start_key, end_key);
  hotspot.EnqueuePartialMerge(cuid, start_key, end_key); 
  Check(hotspot.HasPendingPartialMerge(), "入队后队列应有任务");

  // 3. 弹出一个任务
  PartialMergePendingTask task;
  bool popped = hotspot.PopPendingPartialMerge(&task);
  Check(popped, "应该成功弹出任务");
  Check(task.cuid == cuid && task.scan_first_key == start_key && task.scan_last_key == end_key,
        "弹出的任务字段必须匹配");

  // 4. 第二个任务（检查 HotspotManager 是否做了去重限制）
  // 按照目前设计，测试是否存在隐患：重复 PartialMerge 是否会导致后台进行冗余 I/O？
  bool popped2 = hotspot.PopPendingPartialMerge(&task);
  if (popped2) {
      std::cout << "[WARN] HotspotManager 存在重复 enqueue 导致的冗余任务，建议在 Enqueue 时加入去重限制！" << std::endl;
  } else {
      std::cout << "[PASS] HotspotManager 成功防止了重复任务进入队列。" << std::endl;
  }
}

int main() {
  TestGDCT();
  TestHotspotManagerMergeQueue();
  std::cout << "\nAll Unit Tests Passed Successfully!" << std::endl;
  return 0;
}
