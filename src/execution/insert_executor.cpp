//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
  index_arr_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() {
  if (!(plan_->IsRawInsert())) {
    child_executor_->Init();
  }
  idx_ = 0;
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple next_tuple;
  RID next_rid;
  if (plan_->IsRawInsert()) {
    while (idx_ < plan_->RawValues().size()) {
      std::vector<Value> values = plan_->RawValuesAt(idx_);
      next_tuple = Tuple(values, &table_info_->schema_);
      ++idx_;
      table_info_->table_->InsertTuple(next_tuple, rid, exec_ctx_->GetTransaction());
      Transaction *txn = exec_ctx_->GetTransaction();
      exec_ctx_->GetLockManager()->LockExclusive(txn, *rid);

      for (auto index_info : index_arr_) {
        auto key_attrs = std::vector<uint32_t>{0};
        IndexWriteRecord index_record{*rid,       table_info_->oid_,      WType::INSERT,
                                      next_tuple, index_info->index_oid_, exec_ctx_->GetCatalog()};
        exec_ctx_->GetTransaction()->AppendTableWriteRecord(index_record);
        index_info->index_->InsertEntry(
            next_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, key_attrs), *rid,
            exec_ctx_->GetTransaction());
      }
    }

  } else {
    while (child_executor_->Next(&next_tuple, &next_rid)) {
      table_info_->table_->InsertTuple(next_tuple, rid, exec_ctx_->GetTransaction());
      for (auto index_info : index_arr_) {
        auto key_attrs = std::vector<uint32_t>{0};
        IndexWriteRecord index_record{*rid,       table_info_->oid_,      WType::INSERT,
                                      next_tuple, index_info->index_oid_, exec_ctx_->GetCatalog()};
        exec_ctx_->GetTransaction()->AppendTableWriteRecord(index_record);
        index_info->index_->InsertEntry(
            next_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, key_attrs), *rid,
            exec_ctx_->GetTransaction());
      }
    }
  }
  return false;
}

}  // namespace bustub
