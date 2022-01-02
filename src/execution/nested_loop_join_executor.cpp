//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_executor_->Next(&left_tuple_, &left_rid_);
}
Tuple NestedLoopJoinExecutor::CombineTuple(Tuple *left, Tuple *right) {
  std::vector<Value> values;
  for (auto &col : GetOutputSchema()->GetColumns()) {
    values.push_back(col.GetExpr()->EvaluateJoin(left, left_executor_->GetOutputSchema(), right,
                                                 right_executor_->GetOutputSchema()));
  }
  return Tuple{values, GetOutputSchema()};
}
bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID right_rid;
  if (left_rid_.GetPageId() == INVALID_PAGE_ID) {
    return false;
  }
  while (true) {
    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }
      right_executor_->Init();
      continue;
    }
    if (plan_->Predicate()
            ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                           right_executor_->GetOutputSchema())
            .GetAs<bool>()) {
      *tuple = CombineTuple(&left_tuple_, &right_tuple);
      return true;
    }
  }
}

}  // namespace bustub
