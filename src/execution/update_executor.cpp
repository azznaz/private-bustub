//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
  index_arr_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void UpdateExecutor::Lock(const RID &rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
    exec_ctx_->GetLockManager()->LockUpgrade(txn, rid);
    return;
  }
  if (txn->GetExclusiveLockSet()->find(rid) == txn->GetExclusiveLockSet()->end()) {
    exec_ctx_->GetLockManager()->LockExclusive(txn, rid);
  }
}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple old_tuple;
  while (child_executor_->Next(&old_tuple, rid)) {
    Lock(*rid);
    Tuple new_tuple = GenerateUpdatedTuple(old_tuple);
    if (!table_info_->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction())) {
      return false;
    }
    for (auto index_info : index_arr_) {
      auto key_attrs = std::vector<uint32_t>{0};
      IndexWriteRecord index_record{*rid,      table_info_->oid_,      WType::UPDATE,
                                    new_tuple, index_info->index_oid_, exec_ctx_->GetCatalog()};
      index_record.old_tuple_ = old_tuple;
      exec_ctx_->GetTransaction()->AppendTableWriteRecord(index_record);
      index_info->index_->DeleteEntry(
          old_tuple.KeyFromTuple(*(child_executor_->GetOutputSchema()), index_info->key_schema_, key_attrs), *rid,
          exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(
          new_tuple.KeyFromTuple(*(child_executor_->GetOutputSchema()), index_info->key_schema_, key_attrs), *rid,
          exec_ctx_->GetTransaction());
    }
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  Tuple new_tuple = Tuple{values, &schema};
  return new_tuple;
}

}  // namespace bustub
