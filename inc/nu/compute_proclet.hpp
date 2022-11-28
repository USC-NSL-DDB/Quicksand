#pragma once

#include <tuple>
#include <vector>

#include "nu/proclet.hpp"
#include "nu/task_range.hpp"
#include "nu/utils/mutex.hpp"

namespace nu {

template <TaskRangeBased TR, typename... States>
class ComputeProclet {
 public:
  ComputeProclet(States... states);
  template <typename RetT>
  std::vector<RetT> compute(RetT (*fn)(TR &, States...), TR task_range);
  TR split_tasks();
  std::size_t remaining_size();

 private:
  std::tuple<States...> states_;
  TR task_range_;
  Mutex mutex_;
};

}  // namespace nu

#include "nu/impl/compute_proclet.ipp"
