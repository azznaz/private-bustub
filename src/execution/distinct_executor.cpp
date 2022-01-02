//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), iterator_(dht_.Begin()) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    DistinctKey key = MakeDistinctKey(&tuple);
    dht_.Insert(key);
  }
  iterator_ = dht_.Begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (iterator_ == dht_.End()) {
    return false;
  }
  DistinctKey key = iterator_.Key();
  *tuple = Tuple(key.distinct_key_, GetOutputSchema());
  ++iterator_;
  return true;
}

}  // namespace bustub
