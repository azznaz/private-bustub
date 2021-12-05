//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  tableMetadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  tableIterator_ = tableMetadata_->table_->Begin(exec_ctx_->GetTransaction());
}

void SeqScanExecutor::Init() {
  tableIterator_ = tableMetadata_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple* cur_tuple = nullptr;
  while (!plan_->GetPredicate()->Evaluate(tableIterator_.operator->(),&tableMetadata_->schema_).GetAs<bool>()){
    ++tableIterator_;
    if(tableIterator_ == tableMetadata_->table_->End()){
      return false;
    }
    cur_tuple = tableIterator_.operator->();
  }
  std::vector<Value> values;
  for(auto &col:GetOutputSchema()->GetColumns()){
      values.push_back(col.GetExpr()->Evaluate(cur_tuple,&tableMetadata_->schema_));
  }
  *tuple = Tuple(values,GetOutputSchema());
  *rid = cur_tuple->GetRid();
  return true;
}

}  // namespace bustub
