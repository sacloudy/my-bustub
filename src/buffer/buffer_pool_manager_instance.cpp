//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  //  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].is_dirty_ && pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  int new_page_id = AllocatePage();
  // 先从freelist中找
  //    更新页表
  // 再从replacer中找
  //    更新page_table(删旧增新,可能还会包含刷脏的过程)
  // 希望把上面这个过程提成一个函数find_replace()
  frame_id_t frame_id = FindReplace();
  *page_id = new_page_id;
  if (frame_id == -1) {
    return nullptr;
  }
  page_table_->Insert(new_page_id, frame_id);
  // 谨记andy在头文件里写的注释, 这里该Update the new page metadata, zero out the memory and return the page pointer.
  InitNewPage(frame_id, new_page_id);

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {  // 找到page_id对应的frame_id了
    // lock
    pages_[frame_id].pin_count_++;
    replacer_->SetEvictable(frame_id, false);  // 这个bug找的有点辛苦了:hack下来TEST(FetchPage)debug了一遍才发现
    // setEvictable就应该永远紧跟在pin_count变化之后, UnpinPgImp也是紧跟
    // pin_count++还不用特判pin_count是不是原来就>0(i.e.evictable本来就是false),更应该紧跟!
    replacer_->RecordAccess(frame_id);
    return &pages_[frame_id];
  }

  // 当前页不在buffer pool中, 该读磁盘了, 但也要小心buffer pool满了且无法替换的情况
  frame_id_t replace_fid = FindReplace();
  if (replace_fid == -1) {
    return nullptr;
  }
  page_table_->Insert(page_id, replace_fid);
  InitNewPage(replace_fid, page_id);
  replacer_->RecordAccess(replace_fid);  // 忘了这个导致183行的page0 = bpm->FetchPage(0);执行完history只有9个元素!
  disk_manager_->ReadPage(page_id, pages_[replace_fid].GetData());
  return &pages_[replace_fid];
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;  // 语义相符
  }
  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }
  page_table_->Remove(page_id);

  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  // reset metadata
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  //  pages_[frame_id].ResetMemory();
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ |= is_dirty;  // 原本就脏/现在要设置为脏 有其一就为脏啦
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

// 自建函数(主要是因为FetchPg和NewPg的逻辑的相似程度)
// 如果free_list_为空而且lru_replacer也都被pin住了就返回-1
auto BufferPoolManagerInstance::FindReplace() -> frame_id_t {  // 不这样写的话:cling-tidy:use a trailing return type.
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    // 1. 先从空闲链表
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    // 2. 再从替换器replacer中找
    if (!replacer_->Evict(&frame_id)) {
      return -1;
    }

    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_->Remove(pages_[frame_id].GetPageId());
  }
  return frame_id;
}

void BufferPoolManagerInstance::InitNewPage(frame_id_t frame_id, page_id_t page_id) {
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  // 感觉不用zero out the memory也行吧, 不过还是加上吧, 不然debug的时候不好看
  pages_[frame_id].ResetMemory();  // 比如fetch进来的新页内存中还是放着旧页的内容不太好, 见TEST(FetchPage)
}

}  // namespace bustub
