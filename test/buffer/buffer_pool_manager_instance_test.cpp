//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerInstanceTest, BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[BUSTUB_PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[BUSTUB_PAGE_SIZE / 2] = '\0';
  random_binary_data[BUSTUB_PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(BufferPoolManagerInstanceTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }  // 10个frame都pin住要访问了, free_list为空了, 加入lru的history中但是evictable=false,所以其curr_size=0;

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }  // 但会花掉自增的page_id, 之后再NewPage要从20开始了

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }  // 现在lru中history的cur_size=5了
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }  // 替换掉4个lru中evictable=true的frame(至于lru中最后留下的是哪个frame我们不得而知5?) page0123肯定是被换出了

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);  // page=0要fetch进来把page4踢出去并占掉其所在的frame
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// hack
TEST(BufferPoolManagerInstanceTest, FetchPage) {  // NOLINT
  page_id_t temp_page_id;
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(10, disk_manager, 5);

  std::vector<Page *> pages;  // 这个对观察buffer_pool的page_[]非常有帮助!弥补了bpm中page_无法显示所有页面数据的遗憾!
  std::vector<page_id_t> page_ids;
  std::vector<std::string> content;

  for (int i = 0; i < 10; ++i) {
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    strcpy(new_page->GetData(), std::to_string(i).c_str());  // NOLINT
    pages.push_back(new_page);
    page_ids.push_back(temp_page_id);
    content.push_back(std::to_string(i));
  }
  // lru_replacer的curr_size = 0;
  for (int i = 0; i < 10; ++i) {
    auto *page = bpm->FetchPage(page_ids[i]);  // pin_count=2了
    ASSERT_NE(nullptr, page);
    ASSERT_EQ(pages[i], page);
    ASSERT_EQ(0, std::strcmp(std::to_string(i).c_str(), (page->GetData())));
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));  // pin_count=1
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));  // setEvictable(true)
    bpm->FlushPage(page_ids[i]);                      // is_dirty = false
  }
  // unpin两次lru的curr_size=10了
  for (int i = 0; i < 10; ++i) {
    auto *new_page = bpm->NewPage(&temp_page_id);  // 执行完这里lru的curr_size=9了(pin_count=1)
    ASSERT_NE(nullptr, new_page);
    bpm->UnpinPage(temp_page_id, true);  // 执行完这里curr_size又恢复10了(pin_count=0)
  }
  // 此时buffer_pool中装的是page_id为[10,20)的页, 但它们都没有被pin住,接下来我要再把[0,10)fetch回来了
  for (int i = 0; i < 10; ++i) {
    auto *page = bpm->FetchPage(page_ids[i]);
    ASSERT_NE(nullptr, page);
  }
  // 直接fetch回来page[0,9]的pin_count都=1, 接下来就是针对某些页但操作啦, 开始需要烧脑模拟了
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[4], true));
  auto *new_page = bpm->NewPage(&temp_page_id);  // page20替换page4
  ASSERT_NE(nullptr, new_page);
  ASSERT_EQ(nullptr, bpm->FetchPage(page_ids[4]));  // page20已经把page4原来所占的frame占死了(我可没有unpin)

  // Check Clock
  auto *page5 = bpm->FetchPage(page_ids[5]);  // pin_count=2了(是不是和hit_count保持同步啊)
  auto *page6 = bpm->FetchPage(page_ids[6]);
  auto *page7 = bpm->FetchPage(page_ids[7]);
  ASSERT_NE(nullptr, page5);
  ASSERT_NE(nullptr, page6);
  ASSERT_NE(nullptr, page7);
  strcpy(page5->GetData(), "updatedpage5");  // NOLINT 你这不设置脏位, 换出去就丢掉修改了...
  strcpy(page6->GetData(), "updatedpage6");  // NOLINT
  strcpy(page7->GetData(), "updatedpage7");  // NOLINT
  // page567 pin_count=2
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[5], false));
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[6], false));
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[7], false));
  // page567 pin_count=1 -> curr_size依然是0
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[5], false));
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[6], false));
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[7], false));
  // page567 pin_count=0 -> curr_size=3了

  // page5 would be evicted.
  new_page = bpm->NewPage(&temp_page_id);
  ASSERT_NE(nullptr, new_page);
  // page6 would be evicted.
  page5 = bpm->FetchPage(page_ids[5]);
  ASSERT_NE(nullptr, page5);
  ASSERT_EQ(0, std::strcmp("5", (page5->GetData())));  // 你看,确实updatedpage5没有被写回磁盘吧
  page7 = bpm->FetchPage(page_ids[7]);
  ASSERT_NE(nullptr, page7);
  ASSERT_EQ(0, std::strcmp("updatedpage7", (page7->GetData())));
  // All pages pinned
  ASSERT_EQ(nullptr, bpm->FetchPage(page_ids[6]));  // 全pin住这里就应该fetch不进来的, 这个bug找的比较幸苦了
  bpm->UnpinPage(temp_page_id, false);
  page6 = bpm->FetchPage(page_ids[6]);
  ASSERT_NE(nullptr, page6);
  ASSERT_EQ(0, std::strcmp("6", page6->GetData()));

  strcpy(page6->GetData(), "updatedpage6");  // NOLINT

  // Remove from LRU and update pin_count on fetch
  new_page = bpm->NewPage(&temp_page_id);
  ASSERT_EQ(nullptr, new_page);

  ASSERT_EQ(1, bpm->UnpinPage(page_ids[7], false));
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[6], false));

  new_page = bpm->NewPage(&temp_page_id);
  ASSERT_NE(nullptr, new_page);
  page6 = bpm->FetchPage(page_ids[6]);
  ASSERT_NE(nullptr, page6);
  ASSERT_EQ(0, std::strcmp("updatedpage6", page6->GetData()));
  page7 = bpm->FetchPage(page_ids[7]);
  ASSERT_EQ(nullptr, page7);
  bpm->UnpinPage(temp_page_id, false);
  page7 = bpm->FetchPage(page_ids[7]);
  ASSERT_NE(nullptr, page7);
  ASSERT_EQ(0, std::strcmp("7", (page7->GetData())));

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}
}  // namespace bustub
