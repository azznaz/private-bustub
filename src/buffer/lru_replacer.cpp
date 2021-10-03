//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { max_num_pages_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  latch_.lock();
  if (lru_.empty()) {
    latch_.unlock();
    return false;
  }
  *frame_id = lru_.front();
  mp_.erase(*frame_id);
  lru_.pop_front();
  latch_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();
  if(mp_.count(frame_id) !=0 ){
    lru_.erase(mp_[frame_id]);
    mp_.erase(frame_id);
  }
  latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();
  if(mp_.count(frame_id) == 0){
    lru_.push_back(frame_id);
    auto iter = lru_.end();
    iter--;
    mp_[frame_id] = iter;
  }
  latch_.unlock();
}

size_t LRUReplacer::Size() {
  latch_.lock();
  size_t sz = lru_.size();
  latch_.unlock();
  return sz;
}

}  // namespace bustub
