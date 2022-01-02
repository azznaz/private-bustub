//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    AggregateKey key = MakeAggregateKey(&tuple);
    AggregateValue value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (aht_iterator_ == aht_.End()) {
      return false;
    }
    AggregateKey aggregate_key = aht_iterator_.Key();
    AggregateValue aggregate_value = aht_iterator_.Val();
    if (plan_->GetHaving() != nullptr &&
        !plan_->GetHaving()->EvaluateAggregate(aggregate_key.group_bys_, aggregate_value.aggregates_).GetAs<bool>()) {
      ++aht_iterator_;
      continue;
    }
    std::vector<Value> values;
    for (auto &col : GetOutputSchema()->GetColumns()) {
      values.push_back(col.GetExpr()->EvaluateAggregate(aggregate_key.group_bys_, aggregate_value.aggregates_));
    }
    *tuple = Tuple(values, GetOutputSchema());
    ++aht_iterator_;
    return true;
  }
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
