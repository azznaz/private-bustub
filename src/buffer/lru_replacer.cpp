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
  //lru_latch_.lock();
  if (lru_.empty()) {
    //lru_latch_.unlock();
    return false;
  }
  *frame_id = lru_.front();
  lru_.pop_front();
  //lru_latch_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  //lru_latch_.lock();
  for (auto iter = lru_.begin(); iter != lru_.end(); iter++) {
    if ((*iter) == frame_id) {
      lru_.erase(iter);
      break;
    }
  }
  //lru_latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  //lru_latch_.lock();
  bool flag = false;
  for (auto ele : lru_) {
    if (ele == frame_id) {
      flag = true;
      break;
    }
  }
  if (!flag) {
    lru_.push_back(frame_id);
  }
  //lru_latch_.unlock();
}

size_t LRUReplacer::Size() { return lru_.size(); }

}  // namespace bustub
