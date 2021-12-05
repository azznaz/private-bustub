//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
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
}

void UpdateExecutor::Init() {
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if(!child_executor_->Next(tuple,rid)){
    return false;
  }
  Tuple new_tuple = GenerateUpdatedTuple(*tuple);
  if(table_info_->table_->UpdateTuple(new_tuple,*rid,exec_ctx_->GetTransaction())){
    return false;
  }
  std::vector<IndexInfo*> index_arr = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for(uint32_t i = 0;i<index_arr.size();i++){
    auto index_info = index_arr[i];
    index_info->index_->DeleteEntry(*tuple,*rid,exec_ctx_->GetTransaction());
    index_info->index_->InsertEntry(new_tuple,*rid,exec_ctx_->GetTransaction());
  }
  return true;
}
}  // namespace bustub
