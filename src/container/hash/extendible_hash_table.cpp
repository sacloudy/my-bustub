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
    : global_depth_(1), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.resize(2);
  dir_[0] = std::make_shared<Bucket>(bucket_size, 1);  // 那上面初始化列表中的bucket_size有什么用呢
  dir_[1] = std::make_shared<Bucket>(bucket_size, 1);  //   还是对C++不太熟悉(内部类)
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  //  int bucketId = hash(key, global_depth_);
  std::shared_ptr<Bucket> maybe_bucket = dir_[IndexOf(key)];  // auto的话下面就点不进去啦！
  return maybe_bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::shared_ptr<Bucket> maybe_bucket = dir_[IndexOf(key)];  // auto的话下面就点不进去啦！
  return maybe_bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> guard(latch_);
  //  int bucket_id = IndexOf(key);                             // global_depth_
  //  std::shared_ptr<Bucket> target_bucket = dir_[bucket_id];  // 不想用auto啊
  // //  if (!dir_[bucket_id]->IsFull()) { // 一般先处理特殊逻辑
  // //    dir_[bucket_id]->Insert(key, value);
  // //  } else {}

  // 桶满, 需要桶分裂,也许还需要目录分裂
  //  while (target_bucket->IsFull()) {  // li是改变dir_.size()扩大到可以容纳增长的size_
  while (dir_[IndexOf(key)]->IsFull()) {
    // 再把变量补回来哈哈
    int bucket_id = IndexOf(key);
    std::shared_ptr<Bucket> target_bucket = dir_[bucket_id];
    // 检查是否需要目录扩容
    if (target_bucket->GetDepth() == GetGlobalDepthInternal()) {
      global_depth_++;
      size_t capacity = dir_.size() << 1;
      dir_.resize(capacity);
      for (size_t i = capacity / 2; i < dir_.size(); i++) {  // 本来用的是int: 不转error？C-style cast warning？
        dir_[i] = dir_[i - capacity / 2];  // 先都指向旧桶, 后面再根据元素的具体分配情况改变指针指向
      }
    }
    // 桶分裂(无论是否经过目录扩容逻辑都一样了:dir有多个指针指向将要分裂的桶)
    int mask = 1 << GetLocalDepthInternal(bucket_id);
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
    if (!zero_bucket->GetItems().empty() && !one_bucket->GetItems().empty()) {
      num_buckets_++;
    }
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == target_bucket) {
        if ((i & mask) == 0U) {
          dir_[i] = zero_bucket;
        } else {
          dir_[i] = one_bucket;
        }
      }
    }
    //    if(!zero_bucket->GetItems().empty())
    //      dir_[bucket_id] = zero_bucket;
    //    if(!one_bucket->GetItems().empty())
    //      dir_[bucket_id + capacity] = one_bucket; 之前桶分裂逻辑也错误的放在目录扩容里面了
  }

  // 桶不满, 直接插入(有重复就覆盖)
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
