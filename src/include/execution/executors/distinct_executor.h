//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"

struct DistinctKey {
  std::vector<bustub::Value> distinct_key_;

  bool operator==(const DistinctKey &other) const {
    for (uint32_t i = 0; i < other.distinct_key_.size(); i++) {
      if (distinct_key_[i].CompareEquals(other.distinct_key_[i]) != bustub::CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<DistinctKey> {
  std::size_t operator()(const DistinctKey &distinctKey) const {
    size_t curr_hash = 0;
    for (const auto &key : distinctKey.distinct_key_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std
namespace bustub {
class DistinctHashTable {
 public:
  void Insert(const DistinctKey &key) {
    if (ht_.count(key) == 0) {
      ht_[key] = true;
    }
  }
  class Iterator {
   public:
    explicit Iterator(std::unordered_map<DistinctKey, bool>::const_iterator iter) : iter_{iter} {}

    const DistinctKey &Key() { return iter_->first; }

    Iterator &operator++() {
      ++iter_;
      return *this;
    }

    bool operator==(const Iterator &other) { return this->iter_ == other.iter_; }

    bool operator!=(const Iterator &other) { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<DistinctKey, bool>::const_iterator iter_;
  };

  Iterator Begin() { return Iterator{ht_.cbegin()}; }

  Iterator End() { return Iterator{ht_.cend()}; }

 private:
  std::unordered_map<DistinctKey, bool> ht_;
};
/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  DistinctKey MakeDistinctKey(const Tuple *tuple) {
    std::vector<Value> keys;
    for (size_t i = 0; i < child_executor_->GetOutputSchema()->GetColumnCount(); i++) {
      keys.push_back(tuple->GetValue(child_executor_->GetOutputSchema(), i));
    }
    return {keys};
  }
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  DistinctHashTable dht_;
  DistinctHashTable::Iterator iterator_;
};

}  // namespace bustub
