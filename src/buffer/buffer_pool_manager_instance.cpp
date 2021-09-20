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
      instance_index < pool_size,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  // printf("new a pbi instance_index: %d\n",instance_index);
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);
  // printf("new a bpi %d\n",instance_index);
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManagerInstance::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();
  for (size_t i = 0; i < pool_size_; i++) {
    Page *p = &(pages_[i]);
    p->RLatch();
    if (p->page_id_ == page_id) {
      p->RUnlatch();
      p->WLatch();
      p->pin_count_++;
      replacer_->Pin(page_table_[page_id]);
      p->WUnlatch();
      latch_.unlock();
      return p;
    }
    p->RUnlatch();
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
    latch_.unlock();
    return nullptr;
  }
  Page *r = &(pages_[frame_id]);
  r->RLatch();
  if (r->is_dirty_) {
    disk_manager_->WritePage(r->page_id_, r->data_);
  }
  page_table_.erase(r->page_id_);
  r->RUnlatch();
  r->WLatch();
  r->ResetMemory();
  r->is_dirty_ = false;
  r->page_id_ = INVALID_PAGE_ID;
  r->pin_count_ = 0;
  Page *p = r;
  p->page_id_ = page_id;
  p->pin_count_ = 1;
  replacer_->Pin(page_table_[page_id]);
  disk_manager_->ReadPage(p->page_id_, p->data_);
  p->WUnlatch();
  page_table_[page_id] = frame_id;
  latch_.unlock();
  return p;
}

bool BufferPoolManagerInstance::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  size_t i;
  Page *p;
  for (i = 0; i < pool_size_; i++) {
    p = &(pages_[i]);
    p->RLatch();
    if (p->page_id_ == page_id) {
      p->RUnlatch();
      break;
    }
    p->RUnlatch();
  }
  if (i == pool_size_) {
    // printf("error in buffer_pool unpin!\n");
    latch_.unlock();
    return false;
  }
  p->WLatch();
  p->pin_count_ = p->pin_count_ - 1 >= 0 ? p->pin_count_ - 1 : 0;
  p->WUnlatch();
  p->RLatch();
  if (p->pin_count_ == 0) {
    replacer_->Unpin(page_table_[page_id]);
  }
  p->RUnlatch();
  p->WLatch();
  p->is_dirty_ = is_dirty;
  p->WUnlatch();
  latch_.unlock();
  return true;
}

bool BufferPoolManagerInstance::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  Page *p;
  size_t i;
  for (i = 0; i < pool_size_; i++) {
    p = &(pages_[i]);
    p->RLatch();
    if (p->page_id_ == page_id) {
      break;
    }
    p->RUnlatch();
  }
  if (i == pool_size_) {
    latch_.unlock();
    return false;
  }

  disk_manager_->WritePage(page_id, p->data_);
  p->RUnlatch();
  latch_.unlock();
  return true;
}

Page *BufferPoolManagerInstance::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  // printf("%d newpage\n",instance_index_);
  latch_.lock();
  *page_id = AllocatePage();
  bool flag = false;
  size_t i;

  for (i = 0; i < pool_size_; i++) {
    Page *p = &(pages_[i]);
    p->RLatch();
    if (p->pin_count_ == 0) {
      flag = true;
      p->RUnlatch();
      break;
    }
    p->RUnlatch();
  }
  if (!flag) {
    latch_.unlock();
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
    latch_.unlock();
    return nullptr;
  }
  // page_table_[*page_id] = frame_id;
  // Page *p = pages_ + sizeof(Page)*frame_id;
  Page *p = &(pages_[frame_id]);
  // std::cout<<frame_id<<" "<<pool_size_<<" "<<*page_id<<std::endl;
  // std::cout<<p<<" "<<pages_+frame_id<<std::endl;
  p->RLatch();
  if (p->is_dirty_) {
    disk_manager_->WritePage(p->page_id_, p->data_);
  }
  page_table_.erase(p->page_id_);
  p->RUnlatch();

  p->WLatch();
  p->page_id_ = *page_id;
  p->pin_count_ = 0;
  p->is_dirty_ = false;
  p->ResetMemory();
  p->WUnlatch();
  page_table_[*page_id] = frame_id;
  latch_.unlock();
  // printf("ending:%d new page %d\n",instance_index_,*page_id);
  return p;
}

bool BufferPoolManagerInstance::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (p).
  // 1.   If p does not exist, return true.
  // 2.   If p exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, p can be deleted. Remove p from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  DeallocatePage(page_id);
  Page *p;
  size_t i;
  for (i = 0; i < pool_size_; i++) {
    p = &(pages_[i]);
    p->RLatch();
    if (p->page_id_ == page_id) {
      break;
    }
    p->RUnlatch();
  }
  if (i == pool_size_) {
    latch_.unlock();
    return true;
  }
  if (p->pin_count_ > 0) {
    p->RUnlatch();
    latch_.unlock();
    return false;
  }
  if (p->is_dirty_) {
    disk_manager_->WritePage(p->page_id_, p->data_);
  }
  page_table_.erase(p->page_id_);
  p->RUnlatch();
  p->WLatch();
  p->page_id_ = INVALID_PAGE_ID;
  p->pin_count_ = 0;
  p->ResetMemory();
  p->WUnlatch();
  free_list_.push_back(page_table_[page_id]);
  latch_.unlock();
  return true;
}

void BufferPoolManagerInstance::FlushAllPagesImpl() {
  // You can do it!
  latch_.lock();
  size_t i;
  for (i = 0; i < pool_size_; i++) {
    Page *p = &(pages_[i]);
    p->RLatch();
    if (p->page_id_ == INVALID_PAGE_ID) {
      p->RUnlatch();
      continue;
    }
    disk_manager_->WritePage(p->page_id_, p->data_);
    p->RUnlatch();
  }
  latch_.unlock();
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  // printf("next_page_id: %d num_instance: %d\n",next_page_id,num_instances_);
  next_page_id_ += num_instances_;
  // std::cout<<next_page_id_<<"\n";
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  // printf("pageid: %d instance_index: %d num_instance: %d\n",page_id,instance_index_,num_instances_);
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
