#pragma once

#include <optional>
#include <tuple>
#include <type_traits>
#include <vector>

#include "nu/proclet.hpp"
#include "nu/task_range.hpp"
#include "nu/utils/mutex.hpp"

namespace nu {

template <TaskRangeBased TR, typename RetT>
using compute_proclet_result =
    std::conditional_t<std::is_void_v<RetT>, typename TR::Key,
                       std::pair<typename TR::Key, RetT>>;

template <TaskRangeBased TR, typename... States>
class ComputeProclet {
 public:
  ComputeProclet(States... states);
  template <typename RetT>
  std::optional<compute_proclet_result<TR, RetT>> compute(RetT (*fn)(TR &,
                                                                     States...),
                                                          TR task_range);
  template <typename RetT>
  std::optional<compute_proclet_result<TR, RetT>> steal_and_compute(
      WeakProclet<ComputeProclet> victim, RetT (*fn)(TR &, States...));
  TR steal_tasks();
  void abort();
  std::size_t remaining_size();
  std::size_t processed_size();

 private:
  std::tuple<States...> states_;
  TR task_range_;
  Mutex mutex_;
};

}  // namespace nu

#include "nu/impl/compute_proclet.ipp"
