//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);  // page_id_ = page_id; 除非把基类的page_id设成protected
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array offset)
 * 这个是andy提供的函数哦, 注意internal page中的ValueType就是page_id_t
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index >= 0 && index < GetSize());
  return array_[index].second;
}

/**
 * Find and return the child pointer(page_id) which points to the child page that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 再进一步的语义就是: 查找internal page中小于等于target的最大key对应的value(page_id),
 *    如果page中所有的键值都大于key了就返回array_[0], 反正不可能需要返回-1的:
 *    (本来还说internal的lookup不需要bool的返回值因为如果没找到page_id_t可以设成-1,但是后来发现internal不存在找不到的情况...)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUp(const KeyType &key, KeyComparator comparator) -> page_id_t {
  int l = 0;  // key0是无效的, 也就是不能和target_key作比较的, 但又是在解空间中的:代表target_key比internal中最小元素还小
  // 给l赋0时有点担心后面mid会不会取到0,后面mid补上+1了才不担心了(非常类似于lc35中的r=nums.size():不能参与运算但是在解空间中)
  int r = GetSize() - 1;
  while (l < r) {
    int mid = (l + r + 1) / 2;
    if (comparator(array_[mid].first, key) <= 0) {
      l = mid;
    } else {
      r = mid - 1;
    }
  }
  return array_[l].second;
}

/**
 * Split中调用, 先准备一个new_page, 然后full_page->MoveHalfTo(new_page)这样调用
 * Split上用泛型了, MoveHalfTo可是internal和leaf各自写了一个,相同点不说了,直接来看不同点:
 *      ① leaf版本 主要涉及修改两个leaf_page的NextPageId的链表操作
 *      ② internal版本 主要涉及修改孩子节点的parent_id,将原来指向旧的full_page的节点指向新的recipient
 * @param recipient
 * @param bpm B+Tree中调用时将其bpm传入页节点: 让internal_page操作其孩子节点时的FetchPage用
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm) -> void {
  assert(recipient != nullptr);
  assert(GetSize() == GetMaxSize() + 1);  // 这里其实有内存越界的隐患了,本来internal_page到了max_size就应该分裂的
  int total = GetSize();
  // copy last half to recipient page
  int copy_start = total / 2;  // 把后一(多)半节点move到后面的节点上
  for (int i = copy_start; i < total; i++) {
    recipient->array_[i - copy_start].first = array_[i].first;  // i==copy_idx时做了无效赋值
    recipient->array_[i - copy_start].second = array_[i].second;
    // LOG_DEBUG("#move key%d", array_[i - copy_start].first); 靠,泛型怎么printf呀,还是先用cout当log打下吧
    // std::cout << "#   move key " << array_[i].first << " to new page" << std::endl;

    // update children's parent page
    // 一开始忘了这步,可视化B+树中的指针都不对: 这说明可视化中父到子的指针竟然是用的child的parent_id画出来的
    auto child_raw_page = bpm->FetchPage(array_[i].second);
    auto child_tree_page = reinterpret_cast<BPlusTreePage *>(child_raw_page->GetData());
    child_tree_page->SetParentPageId(recipient->GetPageId());
    bpm->UnpinPage(array_[i].second, true);  // 再强化一下fetch会pin_count++的,用完一定要Unpin
    LOG_DEBUG("Modify the parent_id of child %d(page_id)", child_tree_page->GetPageId());
  }
  recipient->SetSize(total - copy_start);
  SetSize(copy_start);  // 算是惰性删除吧, 总数还是total不能变
}

/**
 * 在本internal_page安全地插入一个kv对<new_key, new_page_id>
 * 安全意味着插入前一定不会满, 也即调用前要检查是否需要split的
 * @param old_page_id 就是函数名InsertAfterPageID中的PageID
 * @param new_key 和new_page_id组成待插入的kv对
 * @param new_page_id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAfterPageID(page_id_t old_page_id, const KeyType &new_key,
                                                       page_id_t new_page_id) -> void {
  int idx = ValueIndex(old_page_id) + 1;  // idx就是<new_key, new_page_id>应该插入的位置
  assert(idx > 0);  // 防御式编程:保证old_page_id是在当前页中的（有检测出错误的）
  IncreaseSize(1);
  int cur_size = GetSize();
  for (int i = cur_size - 1; i > idx; i--) {
    array_[i].first = array_[i - 1].first;
    array_[i].second = array_[i - 1].second;
  }
  array_[idx].first = new_key;
  array_[idx].second = new_page_id;
}

/**
 * 根据value找index (只有InsertAfterPageID中会调用这么奇葩的接口)
 * @param page_id
 * @return 以给定page_id为value的kv的index (再其后插入子节点传上来的page_id就可以了)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(page_id_t page_id) -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (page_id != ValueAt(i)) {
      continue;
    }
    return i;
  }
  return -1;
}

/**
 * InsertIntoParent()的递归终止条件中的逻辑:
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * @param old_value 原来满了的root_id
 * @param new_key new_value页的第一个key
 * @param new_value 存放原来root_page的后一半kv(分裂完成后新root_page的右孩子)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) -> void {
  //  array_[0].second = old_value; flexible array只能用变量做下标?
  //  array_[1].first = new_key;
  //  array_[1].second = new_value;
  int index = 0;
  array_[index++].second = old_value;
  array_[index].first = new_key;
  array_[index].second = new_value;
  SetSize(2);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
