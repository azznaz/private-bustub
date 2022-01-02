//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}
void SeqScanExecutor::LockShared(const RID &rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    return;
  }
  if (txn->GetExclusiveLockSet()->find(rid) != txn->GetExclusiveLockSet()->end() ||
      txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
    return;
  }
  exec_ctx_->GetLockManager()->LockShared(txn, rid);
}

void SeqScanExecutor::UnLock(const RID &rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
      txn->GetExclusiveLockSet()->find(rid) == txn->GetExclusiveLockSet()->end()) {
    exec_ctx_->GetLockManager()->Unlock(txn, rid);
  }
}

void SeqScanExecutor::Init() { table_iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (table_iterator_ == table_info_->table_->End()) {
    return false;
  }
  LockShared(table_iterator_->GetRid());
  Tuple *cur_tuple = table_iterator_.operator->();

  while (plan_->GetPredicate() != nullptr &&
         !plan_->GetPredicate()->Evaluate(cur_tuple, &table_info_->schema_).GetAs<bool>()) {
    UnLock(table_iterator_->GetRid());
    ++table_iterator_;
    if (table_iterator_ == table_info_->table_->End()) {
      return false;
    }
    LockShared(table_iterator_->GetRid());
    cur_tuple = table_iterator_.operator->();
  }
  std::vector<Value> values;
  for (auto &col : GetOutputSchema()->GetColumns()) {
    Value v = col.GetExpr()->Evaluate(cur_tuple, &(table_info_->schema_));
    values.push_back(v);
  }
  *tuple = Tuple(values, GetOutputSchema());
  *rid = cur_tuple->GetRid();
  UnLock(cur_tuple->GetRid());
  ++table_iterator_;
  return true;
}

}  // namespace bustub
