//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "container/hash/extendible_hash_table.h"
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me! what?
  Page *dp = buffer_pool_manager_->NewPage(&directory_page_id_);
  HashTableDirectoryPage *dir_page = reinterpret_cast<HashTableDirectoryPage *>(dp->GetData());
  dir_page->SetPageId(directory_page_id_);
  page_id_t bucket_page_id;
  buffer_pool_manager_->NewPage(&bucket_page_id);
  dir_page->SetBucketPageId(0, bucket_page_id);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  //  HeaderPage *hp = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  //  hp->GetRootId(name, &directory_page_id_);
  //  HashTableDirectoryPage *dir_page =
  //      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
  //  page_id_t bucket_page_id;
  //  buffer_pool_manager_->NewPage(&bucket_page_id);
  //  dir_page->SetBucketPageId(0, bucket_page_id);
  //  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  //  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  bool flag = bucket_page->GetValue(key, comparator_, result);
  page->RUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  if (bucket_page->IsFull()) {
    table_latch_.RUnlock();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    return SplitInsert(transaction, key, value);
  }
  bool flag = bucket_page->Insert(key, value, comparator_);
  table_latch_.RUnlock();
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t old_page_id = KeyToPageId(key, dir_page);
  Page *old_page = buffer_pool_manager_->FetchPage(old_page_id);
  HASH_TABLE_BUCKET_TYPE *old_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(old_page->GetData());
  old_page->WLatch();
  std::vector<ValueType> result;
  old_bucket_page->GetValue(key, comparator_, &result);
  //  if (old_bucket_page->GetValue(key, comparator_, &result)) {
  //    table_latch_.WUnlock();
  //    old_page->WUnlatch();
  //    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  //    buffer_pool_manager_->UnpinPage(old_page_id, false);
  //    return false;
  //  }
  auto iter = std::find(result.begin(), result.end(), value);
  if (iter != result.end()) {
    table_latch_.WUnlock();
    old_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(old_page_id, false);
    return false;
  }
  bool flag = false;
  std::map<page_id_t, bool> is_dirty;
  while (!flag) {
    uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
    if (dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(bucket_idx)) {
      assert(dir_page->Growth());
    }
    // dir_page->GetGlobalDepth() > dir_page->GetLocalDepth(bucket_idx)
    bucket_idx = KeyToDirectoryIndex(key, dir_page);
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    new_page->WLatch();
    HASH_TABLE_BUCKET_TYPE *new_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page->GetData());
    uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
    uint32_t old_pre = bucket_idx & dir_page->GetLocalDepthMask(bucket_idx);
    uint32_t new_pre = old_pre | (static_cast<uint32_t>(1) << local_depth);
    // uint32_t new_local_depth = local_depth + 1;
    for (uint32_t i = 0; i < dir_page->Size(); i++) {
      if (dir_page->GetBucketPageId(i) == old_page_id) {
        dir_page->IncrLocalDepth(i);
        if ((i & dir_page->GetLocalDepthMask(i)) == new_pre) {
          dir_page->SetBucketPageId(i, new_page_id);
        }
      }
    }
    for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
      if (!old_bucket_page->IsReadable(i) || !old_bucket_page->IsOccupied(i)) {
        continue;
      }
      KeyType bucket_key = old_bucket_page->KeyAt(i);
      ValueType bucket_value = old_bucket_page->ValueAt(i);
      uint32_t idx = KeyToDirectoryIndex(bucket_key, dir_page);
      uint32_t mask = dir_page->GetLocalDepthMask(idx);
      if ((idx & mask) == new_pre) {
        old_bucket_page->RemoveAt(i);
        assert(new_bucket_page->Insert(bucket_key, bucket_value, comparator_));
        is_dirty[old_page_id] = true;
        is_dirty[new_page_id] = true;
      }
    }
    if (dir_page->GetBucketPageId(bucket_idx) == old_page_id) {
      flag = old_bucket_page->Insert(key, value, comparator_);
      if (flag) {
        is_dirty[old_page_id] = true;
      }
      new_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(new_page_id, is_dirty[new_page_id]);
    } else {
      flag = new_bucket_page->Insert(key, value, comparator_);
      if (!flag) {
        old_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(old_page_id, is_dirty[old_page_id]);
        old_page_id = new_page_id;
        old_bucket_page = new_bucket_page;
        old_page = new_page;
      } else {
        is_dirty[new_page_id] = true;
        new_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(new_page_id, is_dirty[new_page_id]);
      }
    }
  }
  table_latch_.WUnlock();
  old_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(old_page_id, true);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  bool flag = bucket_page->Remove(key, value, comparator_);

  if (bucket_page->IsEmpty() && dir_page->GetLocalDepth(KeyToDirectoryIndex(key, dir_page)) != 0) {
    table_latch_.RUnlock();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, flag);
    Merge(transaction, key, value);
  } else {
    table_latch_.RUnlock();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, flag);
  }
  return flag;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t img_bucket_idx = dir_page->GetSplitImageIndex(bucket_idx);
  page_id_t img_bucket_page_id = dir_page->GetBucketPageId(img_bucket_idx);
  if (dir_page->GetLocalDepth(bucket_idx) == 0 || dir_page->GetLocalDepth(img_bucket_idx) == 0 ||
      dir_page->GetLocalDepth(bucket_idx) != dir_page->GetLocalDepth(img_bucket_idx)) {
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    return;
  }
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == img_bucket_page_id) {
      dir_page->DecrLocalDepth(i);
    }
    if (dir_page->GetBucketPageId(i) == bucket_page_id) {
      dir_page->DecrLocalDepth(i);
      dir_page->SetBucketPageId(i, img_bucket_page_id);
    }
  }
  assert(buffer_pool_manager_->DeletePage(bucket_page_id));
  if (dir_page->CanShrink()) {
    dir_page->Shrink();
  }
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
