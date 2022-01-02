//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>
#include "concurrency/transaction_manager.h"

namespace bustub {
std::list<LockManager::LockRequest>::iterator LockManager::AddUpgradeLock(LockRequestQueue *lock_request_queue,
                                                                          LockRequest lock_request, const RID &rid) {
  auto it = lock_request_queue->request_queue_.begin();
  while (it != lock_request_queue->request_queue_.end() && it->txn_id_ != lock_request.txn_id_) {
    it++;
  }
  assert(it != lock_request_queue->request_queue_.end());
  lock_request_queue->request_queue_.erase(it);
  lock_request_queue->share_count_--;
  size_t co = lock_request_queue->share_count_;
  it = lock_request_queue->request_queue_.begin();
  for (size_t i = 0; i < co; i++) {
    it++;
  }
  bool flag = false;
  it = lock_request_queue->request_queue_.insert(it, lock_request);
  auto cur = it;
  it--;
  while (it != lock_request_queue->request_queue_.end()) {
    if (it->txn_id_ > lock_request.txn_id_) {
      auto pre = it;
      assert(it->granted_);
      assert(it->lock_mode_ == LockMode::SHARED);
      lock_request_queue->share_count_--;
      Transaction *txn = TransactionManager::GetTransaction(it->txn_id_);
      txn->GetSharedLockSet()->erase(rid);
      txn->SetState(TransactionState::ABORTED);
      it--;
      lock_request_queue->request_queue_.erase(pre);
      flag = true;
    } else {
      it--;
    }
  }
  if (GrantLock(lock_request_queue) || flag) {
    lock_request_queue->cv_.notify_all();
  }
  return cur;
}
std::list<LockManager::LockRequest>::iterator LockManager::AddExclusiveLock(LockRequestQueue *lock_request_queue,
                                                                            LockRequest lock_request, const RID &rid) {
  bool flag = false;
  auto it = lock_request_queue->request_queue_.end();
  it--;
  //  auto mytxn = TransactionManager::GetTransaction(lock_request.txn_id_);
  while (it != lock_request_queue->request_queue_.end()) {
    if (it->txn_id_ > lock_request.txn_id_) {
      auto pre = it;
      Transaction *txn = TransactionManager::GetTransaction(it->txn_id_);
      if (it->granted_) {
        if (it->lock_mode_ == LockMode::SHARED) {
          lock_request_queue->share_count_--;
          txn->GetSharedLockSet()->erase(rid);
        } else {
          lock_request_queue->wrting_ = INVALID_TXN_ID;
          txn->GetExclusiveLockSet()->erase(rid);
        }
      }
      txn->SetState(TransactionState::ABORTED);
      it--;
      lock_request_queue->request_queue_.erase(pre);
      flag = true;
      //      printf("%d abort %d\n",mytxn->GetTransactionId(),txn->GetTransactionId());
    } else {
      it--;
    }
  }
  lock_request_queue->request_queue_.emplace_back(lock_request);
  if (GrantLock(lock_request_queue) || flag) {
    lock_request_queue->cv_.notify_all();
  }
  auto cur = lock_request_queue->request_queue_.end();
  cur--;
  return cur;
}
std::list<LockManager::LockRequest>::iterator LockManager::AddShareLock(LockRequestQueue *lock_request_queue,
                                                                        LockRequest lock_request, const RID &rid) {
  bool flag = false;
  auto it = lock_request_queue->request_queue_.end();
  it--;
  while (it != lock_request_queue->request_queue_.end()) {
    if (it->lock_mode_ == LockMode::SHARED) {
      it--;
      continue;
    }
    if (it->txn_id_ > lock_request.txn_id_) {
      auto pre = it;
      if (it->granted_) {
        lock_request_queue->wrting_ = INVALID_TXN_ID;
      }
      Transaction *txn = TransactionManager::GetTransaction(it->txn_id_);
      txn->GetExclusiveLockSet()->erase(rid);
      txn->SetState(TransactionState::ABORTED);
      it--;
      lock_request_queue->request_queue_.erase(pre);
      flag = true;
    } else {
      break;
    }
  }
  lock_request_queue->request_queue_.emplace_back(lock_request);
  if (GrantLock(lock_request_queue) || flag) {
    lock_request_queue->cv_.notify_all();
  }
  auto cur = lock_request_queue->request_queue_.end();
  cur--;
  return cur;
}

bool LockManager::GrantLock(LockRequestQueue *lock_request_queue) {
  auto it = lock_request_queue->request_queue_.begin();
  bool flag = false;
  while (it != lock_request_queue->request_queue_.end()) {
    if (it->granted_) {
      it++;
      continue;
    }
    if (lock_request_queue->wrting_ == INVALID_TXN_ID && lock_request_queue->share_count_ == 0) {
      flag = true;
      it->granted_ = true;
      if (it->lock_mode_ == LockMode::SHARED) {
        lock_request_queue->share_count_ = 1;
      } else {
        lock_request_queue->wrting_ = it->txn_id_;
      }
      it++;
    } else if (lock_request_queue->share_count_ > 0) {
      if (it->lock_mode_ == LockMode::SHARED) {
        it->granted_ = true;
        flag = true;
        lock_request_queue->share_count_++;
        it++;
      } else {
        break;
      }
    } else if (lock_request_queue->wrting_ != INVALID_TXN_ID) {
      break;
    }
  }
  return flag;
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::SHARED);
  LockRequestQueue *lock_request_queue = &lock_table_[rid];
  auto cur = AddShareLock(lock_request_queue, lock_request, rid);
  while (txn->GetState() != TransactionState::ABORTED && !cur->granted_) {
    lock_request_queue->cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  //  printf("%d try lock-X\n",txn->GetTransactionId());
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  LockRequestQueue *lock_request_queue = &lock_table_[rid];
  auto cur = AddExclusiveLock(lock_request_queue, lock_request, rid);
  while (txn->GetState() != TransactionState::ABORTED && !cur->granted_) {
    //    printf("%d wait lock-X\n",txn->GetTransactionId());
    lock_request_queue->cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  //  printf("%d get lock-X\n",txn->GetTransactionId());
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  LockRequestQueue *lock_request_queue = &lock_table_[rid];
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  assert(lock_request_queue->share_count_ > 0);
  txn->GetSharedLockSet()->erase(rid);
  auto cur = AddUpgradeLock(lock_request_queue, lock_request, rid);
  while (txn->GetState() != TransactionState::ABORTED && !cur->granted_) {
    lock_request_queue->cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  lock_request_queue->upgrading_ = INVALID_TXN_ID;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  LockRequestQueue *lock_request_queue = &lock_table_[rid];
  auto lock_request = lock_request_queue->request_queue_.begin();
  while (lock_request != lock_request_queue->request_queue_.end() && lock_request->txn_id_ != txn->GetTransactionId()) {
    lock_request++;
  }
  // if (lock_request == lock_request_queue->request_queue_.end()) {
  //   return true;
  // }
  assert(lock_request != lock_request_queue->request_queue_.end());
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  //  LockMode mode = lock_request->lock_mode_;
  if (lock_request->lock_mode_ == LockMode::SHARED) {
    lock_request_queue->share_count_--;
  } else {
    lock_request_queue->wrting_ = INVALID_TXN_ID;
  }
  lock_request_queue->request_queue_.erase(lock_request);
  if (GrantLock(lock_request_queue)) {
    lock_request_queue->cv_.notify_all();
  }
  //  if (mode == LockMode::SHARED && lock_request_queue->share_count_ == 0) {
  //    lock_request_queue->cv_.notify_all();
  //  } else if (mode == LockMode::EXCLUSIVE && lock_request_queue->wrting_ == INVALID_TXN_ID) {
  //    lock_request_queue->cv_.notify_all();
  //  }
  return true;
}

}  // namespace bustub
