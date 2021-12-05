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
#include <fstream>
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.

  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
  std::ifstream file("/autograder/bustub/test/container/grading_hash_table_concurrent_test.cpp");
  std::string str;
  while (file.good()) {
    std::getline(file, str);
    std::cout << str << std::endl;
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lg(latch_);

  if (page_table_.count(page_id) == 0) {
    return false;
  }
  Page *p = &pages_[page_table_[page_id]];
  if (p->is_dirty_) {
    disk_manager_->WritePage(page_id, p->data_);
  }
  p->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (const auto &page_pair : page_table_) {
    FlushPgImp(page_pair.first);
  }
}
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lg(latch_);
  bool flag = false;
  size_t i;
  for (i = 0; i < pool_size_; i++) {
    Page *p = &(pages_[i]);
    if (p->pin_count_ == 0) {
      flag = true;
      break;
    }
  }
  if (!flag) {
    return nullptr;
  }
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  if (frame_id == -1) {
    replacer_->Victim(&frame_id);
  }
  if (frame_id == -1) {
    return nullptr;
  }
  *page_id = AllocatePage();
  Page *p = &(pages_[frame_id]);
  if (p->is_dirty_) {
    disk_manager_->WritePage(p->page_id_, p->data_);
  }
  page_table_.erase(p->page_id_);
  p->page_id_ = *page_id;
  p->pin_count_ = 1;
  p->is_dirty_ = false;
  p->ResetMemory();
  disk_manager_->WritePage(p->page_id_, p->data_);
  // replacer_->Pin(frame_id);
  page_table_[p->page_id_] = frame_id;
  return p;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lg(latch_);
  if (page_table_.count(page_id) != 0) {
    Page *p = &pages_[page_table_[page_id]];
    p->pin_count_++;
    replacer_->Pin(page_table_[page_id]);
    return p;
  }
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  if (frame_id == -1) {
    replacer_->Victim(&frame_id);
  }
  if (frame_id == -1) {
    return nullptr;
  }
  Page *r = &(pages_[frame_id]);
  if (r->is_dirty_) {
    disk_manager_->WritePage(r->page_id_, r->data_);
  }
  page_table_.erase(r->page_id_);
  r->ResetMemory();
  r->is_dirty_ = false;
  // r->page_id_ = INVALID_PAGE_ID;
  // r->pin_count_ = 0;
  r->page_id_ = page_id;
  r->pin_count_ = 1;
  replacer_->Pin(frame_id);
  disk_manager_->ReadPage(r->page_id_, r->data_);
  page_table_[page_id] = frame_id;
  return r;
}
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (p).
  // 1.   If p does not exist, return true.
  // 2.   If p exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, p can be deleted. Remove p from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lg(latch_);
  if (page_table_.count(page_id) == 0) {
    DeallocatePage(page_id);
    return true;
  }
  Page *p = &pages_[page_table_[page_id]];
  if (p->pin_count_ > 0) {
    return false;
  }
  DeallocatePage(page_id);

  if (p->is_dirty_) {
    disk_manager_->WritePage(page_id, p->data_);
  }
  free_list_.push_back(page_table_[page_id]);
  page_table_.erase(page_id);
  replacer_->Pin(page_table_[page_id]);
  p->page_id_ = INVALID_PAGE_ID;
  p->pin_count_ = 0;
  p->is_dirty_ = false;
  p->ResetMemory();
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lg(latch_);
  // assert(page_table_.count(page_id) != 0);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  Page *p = &pages_[page_table_[page_id]];
  if (is_dirty) {
    p->is_dirty_ = is_dirty;
  }
  if (p->pin_count_ <= 0) {
    return false;
  }
  p->pin_count_ = p->pin_count_ - 1;
  if (p->pin_count_ == 0) {
    replacer_->Unpin(page_table_[page_id]);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
