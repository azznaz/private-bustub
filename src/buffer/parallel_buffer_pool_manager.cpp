//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_(num_instances), pool_size_(pool_size) {
  // Allocate and create individual BufferPoolManagerInstances
  this->bpm_table_ = new BufferPoolManager *[num_instances];
  start_index_ = 0;
  for (size_t i = 0; i < num_instances; i++) {
    this->bpm_table_[i] = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; i++) {
    delete this->bpm_table_[i];
  }
  delete[] this->bpm_table_;
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances

  return num_instances_ * pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  size_t index = page_id % num_instances_;
  return this->bpm_table_[index];
}

Page *ParallelBufferPoolManager::FetchPageImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  size_t index = page_id % num_instances_;
  Page *p = this->bpm_table_[index]->FetchPage(page_id);
  return p;
}

bool ParallelBufferPoolManager::UnpinPageImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  size_t index = page_id % num_instances_;
  bool flag = this->bpm_table_[index]->UnpinPage(page_id, is_dirty);
  return flag;
}

bool ParallelBufferPoolManager::FlushPageImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  size_t index = page_id % num_instances_;
  bool flag = this->bpm_table_[index]->FlushPage(page_id);
  return flag;
}

Page *ParallelBufferPoolManager::NewPageImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called

  Page *p;
  size_t pre = start_index_;
  start_index_ = (start_index_ + 1) % num_instances_;
  size_t i = pre;
  do {
    p = this->bpm_table_[i]->NewPage(page_id);
    if (p != nullptr) {
      return p;
    }
    i = (i + 1) % num_instances_;
  } while (i != pre);
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePageImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  size_t index = page_id % num_instances_;
  bool flag = this->bpm_table_[index]->DeletePage(page_id);
  return flag;
}

void ParallelBufferPoolManager::FlushAllPagesImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; i++) {
    this->bpm_table_[i]->FlushAllPages();
  }
}
}  // namespace bustub
