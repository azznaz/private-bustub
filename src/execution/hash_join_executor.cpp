//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  left_child_ = std::move(left_child);
  right_child_ = std::move(right_child);
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  idx_ = 0;
  Tuple tuple;
  RID rid;
  AbstractExpression *right = const_cast<AbstractExpression *>(plan_->RightJoinKeyExpression());
  AbstractPlanNode *right_plan = const_cast<AbstractPlanNode *>(plan_->GetRightPlan());
  while (right_child_->Next(&tuple, &rid)) {
    int32_t key = right->Evaluate(&tuple, right_plan->OutputSchema()).GetAs<int32_t>();
    right_ht_[key].push_back(tuple);
  }
  left_child_->Next(&left_tuple_, &left_rid_);
}

Tuple HashJoinExecutor::CombineTuple(Tuple *left, Tuple *right) {
  std::vector<Value> values;
  for (auto &col : GetOutputSchema()->GetColumns()) {
    values.push_back(
        col.GetExpr()->EvaluateJoin(left, left_child_->GetOutputSchema(), right, right_child_->GetOutputSchema()));
  }
  return Tuple{values, GetOutputSchema()};
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  if (left_rid_.GetPageId() == INVALID_PAGE_ID) {
    return false;
  }
  AbstractExpression *left = const_cast<AbstractExpression *>(plan_->LeftJoinKeyExpression());
  AbstractPlanNode *left_plan = const_cast<AbstractPlanNode *>(plan_->GetLeftPlan());
  int32_t key = left->Evaluate(&left_tuple_, left_plan->OutputSchema()).GetAs<int32_t>();
  while (true) {
    if (idx_ >= right_ht_[key].size()) {
      if (!left_child_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }
      key = left->Evaluate(&left_tuple_, left_plan->OutputSchema()).GetAs<int32_t>();
      idx_ = 0;
      continue;
    }
    right_tuple = right_ht_[key][idx_];
    idx_++;
    *tuple = CombineTuple(&left_tuple_, &right_tuple);
    return true;
  }
}

}  // namespace bustub
