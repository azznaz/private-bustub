//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_arr_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Lock(const RID &rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
    exec_ctx_->GetLockManager()->LockUpgrade(txn, rid);
    return;
  }
  if (txn->GetExclusiveLockSet()->find(rid) == txn->GetExclusiveLockSet()->end()) {
    exec_ctx_->GetLockManager()->LockExclusive(txn, rid);
  }
}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple delete_tuple;
  while (child_executor_->Next(&delete_tuple, rid)) {
    Lock(*rid);
    table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    for (auto index_info : index_arr_) {
      auto key_attrs = std::vector<uint32_t>{0};
      IndexWriteRecord index_record{*rid,         table_info_->oid_,      WType::DELETE,
                                    delete_tuple, index_info->index_oid_, exec_ctx_->GetCatalog()};
      exec_ctx_->GetTransaction()->AppendTableWriteRecord(index_record);
      index_info->index_->DeleteEntry(
          delete_tuple.KeyFromTuple(*(child_executor_->GetOutputSchema()), index_info->key_schema_, key_attrs), *rid,
          exec_ctx_->GetTransaction());
    }
  }
  return false;
}

}  // namespace bustub
