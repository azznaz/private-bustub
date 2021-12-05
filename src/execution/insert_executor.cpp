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
  tableMetadata_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void InsertExecutor::Init() {
  if(plan_->IsRawInsert()){
    child_executor_->Init();
  }
  idx_ = 0;
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
   Tuple next_tuple;
   RID next_rid;
   bool have_next = true;
   if(plan_->IsRawInsert()){
     if(idx_ >= plan_->RawValues().size()) {
       return false;
     }
     std::vector<Value> values = plan_->RawValuesAt(idx_);
     next_tuple = Tuple(values,&tableMetadata_->schema_);
     ++idx_;
   }else{
      if(!child_executor_->Next(&next_tuple,&next_rid)){
        return false;
      }
   }
   std::vector<IndexInfo*> index_arr = exec_ctx_->GetCatalog()->GetTableIndexes(tableMetadata_->name_);
   tableMetadata_->table_->InsertTuple(next_tuple,rid,exec_ctx_->GetTransaction());
   for(uint32_t i = 0;i < index_arr.size();i++){
     auto index_info = index_arr[i];
     index_info->index_->InsertEntry(next_tuple,*rid,exec_ctx_->GetTransaction());
   }
   return true;
}

}  // namespace bustub
