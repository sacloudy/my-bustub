//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_insert_test.cpp
//
// Identification: test/storage/b_plus_tree_insert_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
// xiao给的测试文件, 确实规模大了容易暴露出bpm的问题(准确的来说是对bpm的不当使用问题)
#include <algorithm>
#include <cstdio>
#include <numeric>
#include <random>

#include "buffer/buffer_pool_manager_instance.h"
#include "concurrency/transaction.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT
namespace bustub {
TEST(BPlusTreeTests, ScaleTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(80, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 6, 6);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int size = 1000;
  std::vector<int64_t> keys(size);
  std::iota(keys.begin(), keys.end(), 1);

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);
  std::cout << "---------------------------------------------------" << std::endl;
  int i = 0;
  (void)i;
  for (auto key : keys) {
    i++;
    std::cout << i << std::endl;
    sleep(static_cast<unsigned int>(0.1));
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    // 别小看这个Draw, 有两个作用:
    // ① 画出树可以观察B+树形态, 其实这个不重要, 因为根本找不出错来, 一开始执迷于观察跑挂掉后树的形态了, 其实是随机的,
    //    唯一的规律是挂掉前一刻都是要向满了的叶节点插入数据呢, 而且跑几次就发现另一个有意思的现象:
    //    导致挂掉的节点号非常稳定(应该是B+树特性),如总是i=150附近(如果尝试调大节点的max_size这个相对稳定的i也就变大)
    // ② Draw里面的ToGraph中会遍历整个树然后就发现空指针bug了(本质是页面都pin住了fetch回null了),
    //    如果不是这个Draw空指针直接挂掉的话什么信息都打不出来(也就定位不到bug位置了呗), 不信你注释掉一个Unpin试试
    //    if (i > 120) {
    //      tree.Draw(bpm, "/Users/sacloud/labs/cmu-15445/my-bustub/build/draw" + std::to_string(i) + ".dot");
    //    }
  }
  std::vector<RID> rids;

  std::shuffle(keys.begin(), keys.end(), g);

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);

  int cnt = 1;
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    std::cout << "cnt: " << cnt++ << std::endl;
    //    if (cnt > 250) {
    //      tree.Draw(bpm, "/Users/sacloud/labs/cmu-15445/my-bustub/build/itr" + std::to_string(cnt) + ".dot");
    //    }
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

}  // namespace bustub
