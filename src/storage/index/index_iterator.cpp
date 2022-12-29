/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/logger.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

/**
 * 本来还说新增一个参数为MappingType的构造函数, 这就根本不是迭代器的思路(迭代器什么思路? 指针啊)
 * @param leaf 和index一起组成迭代器指针指向RID
 * @param index
 * @param bpm iter需要bpm的本质原因就是iter是要对页面进行操作的, 和internal_page的MoveHalfTo需要bpm一个道理
 *           ① iterator++时到next_page要把原来的Unpin掉呢吧
 *           ② 外面B+树调用Begin/End()时返回的iter中是要包含pin住对应页面的,所以Unpin的任务自然要交给iter的析构函数做了.
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *bpm)
    : index_(index), leaf_(leaf), bpm_(bpm) {}

// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::~IndexIterator() = default;  // 构造函数的default不用注释掉,析构函数的default可必须注释掉了吧

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_ != nullptr) {
    LOG_DEBUG("析构中的Unpin~");
    bpm_->UnpinPage(leaf_->GetPageId(), false);
  }
}

/**
 * Return whether this iterator is pointing at the last key/value pair.
 */
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize() - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  LOG_DEBUG("PageFetch from operator * :");
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(bpm_->FetchPage(leaf_->GetPageId()));
  //  return leaf->GetItem(index_); 直接return掉你就忘记Unpin了!(在写delete操作前最后一个找到的bug, 挺隐蔽啊...)
  auto &tuple = leaf->GetItem(index_);  // 不加引用可就闹笑话了, 删掉&编译器就报错了
  // 灵剑:返回引用最常见的是返回类的成员的引用，这些成员可能本身是某些private/protected字段的一部分，
  //      但在符合条件时允许外部直接修改，容器类当中很常见。  普通函数也可以返回static变量的引用。
  bpm_->UnpinPage(leaf_->GetPageId(), false);
  return tuple;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;
  if (index_ == leaf_->GetSize()) {
    page_id_t next_page_id = leaf_->GetNextPageId();
    LOG_DEBUG("index_=%d, leaf_.size()=%d相等说明到当前page尾了: next_page:%d", index_, leaf_->GetSize(), next_page_id);
    if (next_page_id != INVALID_PAGE_ID) {  // 没有到最后一个页面才修改leaf_为next_page呢, 否则就不要修改
      LOG_DEBUG("iter通过leaf上的next_page指针到了下一个page, 然后马上Unpin掉前一个leaf");
      bpm_->UnpinPage(leaf_->GetPageId(), false);
      Page *page = bpm_->FetchPage(next_page_id);
      leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
      index_ = 0;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return itr.leaf_ == this->leaf_ && itr.index_ == this->index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  return itr.leaf_ != this->leaf_ || itr.index_ != this->index_;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
