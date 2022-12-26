//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);  // page_id_ = page_id; 除非把基类的page_id设成protected
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);  // 其实单提出去放到StartNewTree中应该也行,毕竟这个操作非常罕见
}

/**
 * 在leaf page中找给定key的对应的RID, 和internal不同的是leaf确实存在找不到的情况了
 * 而且leaf的ValueType是RID, 不太好表示找不到的情况, 所以还是参数返回查询结果喽, 然后return bool表示查询是否成功
 * @return 查询结果的状态
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, KeyComparator comparator) -> bool {
  int l = 0;  // 我知道key0是无效的, 但0也确实在解空间中啊
  int r = GetSize() - 1;
  while (l <= r) {
    int mid = (l + r) / 2;
    if (comparator(array_[mid].first, key) == 0) {
      value = array_[mid].second;
      return true;
    }
    if (comparator(array_[mid].first, key) < 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  return false;
}

/**
 * 在leaf_page中安全地插入一个kv对
 * @param key
 * @param value
 * @param comparator
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator comparator) -> void {
  int idx = KeyIndex(key, comparator);  // 找到插入位置index: first larger than key
  assert(idx >= 0);
  IncreaseSize(1);
  int cur_size = GetSize();
  for (int i = cur_size - 1; i >= idx; i--) {  // 将index后元素统统向后移一位
    array_[i].first = array_[i - 1].first;
    array_[i].second = array_[i - 1].second;
  }
  array_[idx].first = key;
  array_[idx].second = value;
}

/**
 * Split中调用, 先准备一个new_page, 然后full_page->MoveHalfTo(new_page)这样调用
 * Split上用泛型了, MoveHalfTo可是internal和leaf各自写了一个,相同点不说了,直接来看不同点:
 *      ① leaf版本 主要涉及修改两个leaf_page的NextPageId的链表操作
 *      ② internal版本 主要涉及修改孩子节点的parent_id,将原来指向旧的full_page的节点指向新的recipient
 * @param recipient
 * @param bpm leaf_page是用不着这个bpm的, 但是为了在包含泛型的Split中调用,也必须保留这个参数
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *bpm) -> void {
  assert(recipient != nullptr);
  assert(GetSize() == GetMaxSize());  // leaf page达到max_size就已经超啦,所以这是已经插入后的啦
  int total = GetMaxSize();
  // copy last half
  int copy_start = total / 2;  // 把后一(多)半节点move到后面的节点上
  for (int i = copy_start; i < total; i++) {
    recipient->array_[i - copy_start].first = array_[i].first;  // i==copy_idx时做了无效赋值
    recipient->array_[i - copy_start].second = array_[i].second;
  }
  // 纯纯链表操作
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());

  recipient->SetSize(total - copy_start);
  SetSize(copy_start);  // 也算是惰性删除吧, 总和是total不能变
}

/**
 * 找data_中第一个大于key的index
 * @param key
 * @param comparator
 * @return
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, KeyComparator comparator) -> int {
  // 原来为了图快和保证没有bug先写了个傻瓜查找, 后来改成二分了也可以先别删掉后面想用gradescope比较下性能提升
  //  for (int i = 0; i < GetSize(); i++) {
  //    if (comparator(array_[i].first, key) > 0) {
  //      return i;
  //    }
  //  }
  //  return GetSize();

  int l = 0;
  int r = GetSize();
  while (l < r) {
    int mid = l + (r - l) / 2;
    if (comparator(array_[mid].first, key) >= 0) {  // 其实不可能等于的
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return l;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
