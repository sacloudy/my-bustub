//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>  // <K,V> 分别是 <page_id_t, frame_id_t>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  //  dir_.resize(2);
  //  dir_[0] = std::make_shared<Bucket>(bucket_size, 1);  // 那上面初始化列表中的bucket_size有什么用呢
  //  dir_[1] = std::make_shared<Bucket>(bucket_size, 1);  //   还是对C++不太熟悉(内部类)
  // 初始化：全局深度为0，局部深度为0，桶数量为1, 这是andy在函数签名中给的(是chi先生给的555),不能乱改的
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));  // bucket也要初始化大小为0
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  //  std::lock_guard<std::mutex> guard(latch_); 不死锁等啥呢:内部方法不要加锁哦
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::lock_guard<std::mutex> guard(latch_);  // 这里加锁就要保证ExtendibleHashTable别的方法不会调用它
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  //  std::lock_guard<std::mutex> guard(latch_); 不死锁等啥呢:内部方法不要加锁哦
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::lock_guard<std::mutex> guard(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::lock_guard<std::mutex> guard(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  //  std::lock_guard<std::mutex> guard(latch_); 本地测试能过(你不看上面的GetNumBuckets灰着呢),交上去就死锁了
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  //  int bucketId = hash(key, global_depth_);
  std::shared_ptr<Bucket> maybe_bucket = dir_[IndexOf(key)];  // auto的话下面就点不进去啦！
  return maybe_bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  std::shared_ptr<Bucket> maybe_bucket = dir_[IndexOf(key)];  // auto的话下面就点不进去啦！
  return maybe_bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> guard(latch_);
  //  int bucket_id = IndexOf(key);
  //  std::shared_ptr<Bucket> target_bucket = dir_[bucket_id];  // 不想用auto啊
  //  if (!dir_[bucket_id]->IsFull()) { // 首先不能用if啊, 很有可能一次桶分裂桶内已满的元素一个都没有移走
  //    dir_[bucket_id]->Insert(key, value);
  //  } else {桶满扩容}
  // 第二个问题是一个code pattern: 预处理与复用(提前处理特殊逻辑+复用一般逻辑)
  //    写递归逻辑一般先写通用逻辑再写conner case,
  //    但在这里明显特殊逻辑是比通用逻辑多出一个步骤的(桶分裂完还要正常插入呢),所以应该先写特殊逻辑(桶满情况)
  //  while (target_bucket->IsFull()) {  // 额上while也不能这样写...要死循环了...
  while (dir_[IndexOf(key)]->IsFull()) {  // li的改变: 增加global_depth_后算出的桶号就变了
    int bucket_id = IndexOf(key);         // 为了后面写起来方便, 再把变量补回来hh
    std::shared_ptr<Bucket> target_bucket = dir_[bucket_id];
    // 有可能需要目录扩容 (想想如何能再次用上code pattern:预处理与复用)
    if (target_bucket->GetDepth() == GetGlobalDepthInternal()) {  // 去检查下这个方法没有加锁吧
      global_depth_++;
      size_t capacity = dir_.size() << 1;
      dir_.resize(capacity);
      for (size_t i = capacity / 2; i < dir_.size(); i++) {  // 本来用的是int: 不转error？C-style cast warning？
        dir_[i] = dir_[i - capacity / 2];  // 先都指向旧桶, 后面再根据元素的具体分配情况改变指针指向(用上啦)
      }
    }
    // 桶分裂(无论是否经过目录扩容逻辑都一样了:dir有多个指针指向将要分裂的桶)
    int mask = 1 << GetLocalDepthInternal(bucket_id);
    // 逻辑上是要把一部分元素移除旧桶,实际操作上是开两个新桶
    auto zero_bucket = std::make_shared<Bucket>(bucket_size_, GetLocalDepthInternal(bucket_id) + 1);
    auto one_bucket = std::make_shared<Bucket>(bucket_size_, GetLocalDepthInternal(bucket_id) + 1);
    for (auto &[k, v] : target_bucket->GetItems()) {
      size_t hash_key = std::hash<K>()(k);
      if ((hash_key & mask) == 0U) {  // ==0U是clion给我加的hhh
        zero_bucket->Insert(k, v);
      } else {
        one_bucket->Insert(k, v);
      }
    }
    // 下面对num_buckets_的增加操作属实是自作聪明了, 其实就算全部没有搬移也要开新桶呢, 也要把目录指针指向新开的空桶呢
    // if (!zero_bucket->GetItems().empty() && !one_bucket->GetItems().empty()) {
    //   num_buckets_++;  // 全部没有搬移或者全部都搬移都不能走到这里
    // }
    num_buckets_++;
    // 移动目录指针
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == target_bucket) {  // 这里触发次数不一定是两次哦，如果有多个指针指向待分裂的桶就会触发多次的
        if ((i & mask) == 0U) {
          dir_[i] = zero_bucket;
        } else {
          dir_[i] = one_bucket;
        }
      }
    }
  }

  // 终于到一般逻辑了: 桶不满, 直接插入(有重复就覆盖)
  dir_[IndexOf(key)]->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&](std::pair<K, V> p) {
    if (p.first == key) {
      value = p.second;
      return true;
    }
    return false;
  });
  //  for (auto &[k, v] : list_) { // replace loop by 'std::any_of()'
  //    if (k == key) {
  //      value = v;
  //      return true;
  //    }
  //  }
  //  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&](std::pair<K, V> p) {
    if (p.first == key) {
      list_.remove(p);
      return true;
    }
    return false;
  });
  //  for (auto &[k, v] : list_) { // replace loop by 'std::any_of()'
  //    if (k == key) {
  //      list_.remove(std::make_pair(k, v));
  //      return true;
  //    }
  //  }
  //  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  //  for (auto it = list_.begin(); it != list_.end(); ++it) {
  //    if ((*it).first == key) {
  //      (*it).second = value;
  //      return true;
  //    }
  //  }
  for (auto &[k, v] : list_) {
    if (k == key) {
      v = value;
      return false;
    }
  }
  if (list_.size() == size_) {
    return false;
  }
  list_.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
