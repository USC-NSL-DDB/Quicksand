#include <algorithm>
#include <queue>
#include <ranges>
#include <type_traits>

#include "nu/resource_reporter.hpp"
#include "nu/utils/scoped_lock.hpp"
#include "nu/utils/time.hpp"

namespace nu {

template <class TR, typename... States>
inline ComputeProclet<TR, States...>::ComputeProclet(States... states)
    : states_(std::move(states)...) {}

template <class TR, typename... States>
template <typename RetT>
inline std::vector<RetT> ComputeProclet<TR, States...>::compute(
    RetT (*fn)(TR &, States...), TR task_range) {
  std::vector<RetT> rets;
  task_range_ = std::move(task_range);

  while (true) {
    std::apply(
        [&](auto &... states) {
          rets.emplace_back(fn(task_range_, states...));
        },
        states_);

    ScopedLock g(&mutex_);
    if (likely(task_range_.empty())) {
      break;
    }
  }

  return rets;
}

template <class TR, typename... States>
inline TR ComputeProclet<TR, States...>::split_tasks() {
  ScopedLock g(&mutex_);
  return task_range_.split();
}

template <class TR, typename... States>
inline std::size_t ComputeProclet<TR, States...>::remaining_size() {
  ScopedLock g(&mutex_);
  return task_range_.size();
}

}  // namespace nu
