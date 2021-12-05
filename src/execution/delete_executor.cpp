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
}

void DeleteExecutor::Init() {
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if(!child_executor_->Next(tuple,rid)){
    return false;
  }
  if(table_info_->table_->MarkDelete(*rid,exec_ctx_->GetTransaction())){
    return false;
  }
  std::vector<IndexInfo*> index_arr = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for(uint32_t i = 0;i<index_arr.size();i++){
    auto index_info = index_arr[i];
    index_info->index_->DeleteEntry(*tuple,*rid,exec_ctx_->GetTransaction());
  }
  return true;
}

}  // namespace bustub
