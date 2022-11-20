#pragma once

#include <memory>
#include <tuple>
#include <type_traits>
#include <vector>

#include "nu/commons.hpp"
#include "nu/task_range.hpp"
#include "nu/utils/future.hpp"
#include "nu/utils/mutex.hpp"

namespace nu {

template <TaskRangeBased TR, typename... States>
class ComputeProcletWorker {
 public:
  ComputeProcletWorker(States... states);
  template <typename RetT>
  RetT compute(RetT (*fn)(TR &, States...), TR task_range);
  TR steal_work();

 private:
  std::tuple<States...> states_;
  std::unique_ptr<TR> task_range_;
  Mutex mutex_;
};

template <TaskRangeBased TR>
class ComputeProclet {
 public:
  ComputeProclet() = default;
  template <typename RetT, typename... S0s, typename... S1s>
  std::vector<RetT> run(RetT (*fn)(TR &, S0s...), TR task_range,
                        S1s &&... states);
  template <typename RetT, typename... S0s, typename... S1s>
  Future<std::vector<RetT>> run_async(RetT (*fn)(TR &, S0s...), TR task_range,
                                      S1s &&... states);
};

}  // namespace nu

#include "nu/impl/compute_proclet.ipp"
