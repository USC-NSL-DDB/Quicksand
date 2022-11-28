#pragma once

#include <vector>

#include "nu/compute_proclet.hpp"
#include "nu/task_range.hpp"
#include "nu/utils/future.hpp"

namespace nu {

template <typename RetT, TaskRangeBased TR, typename... States>
class DistributedExecutor {
 public:
  std::vector<RetT> &&get();
  DistributedExecutor(const DistributedExecutor &) = delete;
  DistributedExecutor &operator=(const DistributedExecutor &) = delete;
  DistributedExecutor(DistributedExecutor &&) = default;
  DistributedExecutor &operator=(DistributedExecutor &&) = default;

 private:
  Future<std::vector<RetT>> future_;
  template <typename R, TaskRangeBased T, typename... S0s, typename... S1s>
  friend DistributedExecutor<R, T, S0s...> make_distributed_executor(
      R (*fn)(T &, S0s...), T, S1s &&...);

  DistributedExecutor() = default;
  template <typename... S1s>
  std::vector<RetT> run(RetT (*fn)(TR &, States...), TR task_range,
                        S1s &&... states);
  template <typename... S1s>
  void start_async(RetT (*fn)(TR &, States...), TR task_range,
                   S1s &&... states);
};

template <typename RetT, TaskRangeBased TR, typename... S0s, typename... S1s>
DistributedExecutor<RetT, TR, S0s...> make_distributed_executor(
    RetT (*fn)(TR &, S0s...), TR task_range, S1s &&... states);

}  // namespace nu

#include "nu/impl/dis_executor.ipp"
