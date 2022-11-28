//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  hit_count_.resize(replacer_size_ + 1);  // 也没说frame_id是从0还是1开始鸭
  evictable_.resize(replacer_size_ + 1);
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  //  如何为每个页面维护一个访问历史
  size_t count = ++hit_count_[frame_id];
  if (count < k_) {   // 更新其在history_lru中的位置
    if (count > 1) {  // 用history_map_.count(frame_id)>0判断应该也行
      return;         // 不要调整history中的顺序!只要没到k,访问任何元素也不会改变其优先级
    }
    history_list_.emplace_front(frame_id);  // 删的时候先map后list, 存的时候刚好相反
    history_map_[frame_id] = history_list_.begin();
  } else if (hit_count_[frame_id] == k_) {  // 从history移入cache
    auto it = history_map_[frame_id];
    history_map_.erase(frame_id);
    history_list_.erase(it);
    cache_list_.emplace_front(frame_id);  // 删的时候先map后list, 存的时候刚好相反
    cache_map_[frame_id] = cache_list_.begin();
  } else {  // 更新其在cache_lru中的位置
    auto it = cache_map_[frame_id];
    cache_list_.erase(it);
    cache_list_.emplace_front(frame_id);  // 删的时候先map后list, 存的时候刚好相反
    cache_map_[frame_id] = cache_list_.begin();
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  //  auto it = history_list_.rbegin();
  // begin()类型可以是std::list<frame_id_t>::iterator,但rbegin()不是这个(clion咋提示是iter<iter<>>了)
  auto it = history_list_.end();
  it--;
  //  while (!history_map_.empty()) {
  //  不能这样判断,因为evictable设成false的话不会导致history_map中元素变少,全是false就会造成死循环
  while (it != --history_list_.begin()) {  // 同上直接rend()是没法判等的,之后要补充C++容器迭代器的知识哦
    if (evictable_[*it]) {
      *frame_id = *it;  // 这里返回但其实不是局部变量但地址,而是拷贝值了,clion不够智能哇
      auto evict_frame_id = static_cast<frame_id_t>(*it);  // 这里不先解引用了erase了it就失效了
      history_list_.erase(it);  // 终于删除也可以先操作list啦，(ps:rbegin但话这里也填不进去)
      history_map_.erase(evict_frame_id);
      // 记得改lru的meta data鸭
      curr_size_--;
      // 记得要改frame的meta data鸭
      hit_count_[evict_frame_id] = 0;      // evictable_[*it]应该无所谓吧,反正也已经移除了
      evictable_[evict_frame_id] = false;  // (不行的,这样下次setEvictable(true)的时候就不会让cur_size++了)
      return true;
    }
    it--;
  }
  it = cache_list_.end();
  it--;
  //  while (!cache_map_.empty()) { 同理不行
  while (it != --cache_list_.begin()) {
    if (evictable_[*it]) {
      *frame_id = *it;
      auto evict_frame_id = static_cast<frame_id_t>(*it);
      cache_list_.erase(it);
      cache_map_.erase(evict_frame_id);
      curr_size_--;
      hit_count_[evict_frame_id] = 0;
      evictable_[evict_frame_id] = false;
      return true;
    }
    it--;
  }
  return false;
}

// 一开始没太想明白SetEvictable的思路，
// 是需要给每个frame_id都建立一个元数据信息吗(比如bool类型的evictable)，那每个frame必须调用setEvictable?(这个讨论见.h文件)
// 在cache_lru中的frame被setEvict两次应该回到哪里呢？(想复杂了,就回原地,关键是可以不真正移动元素呢,只需在evict前判断下该frame_id是否可以移除即可)
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (hit_count_[frame_id] == 0) {  // 保护措施: 以防用户对不存在的frame做操作而改变lru相关结构
    return;
  }
  // evictable -> unevictable
  if (evictable_[frame_id] && !set_evictable) {
    evictable_[frame_id] = false;
    curr_size_--;
  }
  // evictable -> unevictable
  if (!evictable_[frame_id] && set_evictable) {
    evictable_[frame_id] = true;
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (!evictable_[frame_id]) {
    return;  // throw an exception
  }
  // 不动free_list? 额, 你忘了lru_replacer和free_list是平级关系了？
  if (hit_count_[frame_id] < k_) {
    auto it = history_map_[frame_id];
    history_list_.erase(it);
    history_map_.erase(frame_id);
  } else {
    auto it = cache_map_[frame_id];
    cache_list_.erase(it);
    cache_map_.erase(frame_id);
  }
  curr_size_--;

  hit_count_[frame_id] = 0;  // hit_count_应该还是不用管
  evictable_[frame_id] = false;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
