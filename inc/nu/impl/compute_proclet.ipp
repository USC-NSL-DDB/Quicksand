#include <algorithm>
#include <queue>
#include <ranges>
#include <type_traits>

#include "nu/resource_reporter.hpp"
#include "nu/utils/scoped_lock.hpp"
#include "nu/utils/time.hpp"

namespace nu {

template <TaskRangeBased TR, typename... States>
inline ComputeProclet<TR, States...>::ComputeProclet(States... states)
    : states_(std::move(states)...) {}

template <TaskRangeBased TR, typename... States>
template <typename RetT>
inline std::optional<compute_proclet_result<TR, RetT>>
ComputeProclet<TR, States...>::compute(RetT (*fn)(TR &, States...),
                                       TR task_range) {
  if (unlikely(task_range.empty())) {
    return std::nullopt;
  }
  {
    ScopedLock g(&mutex_);

    task_range_ = std::move(task_range);
  }
  auto l_key = task_range_.l_key();

  if constexpr (std::is_void_v<RetT>) {
    std::apply([&](auto &...states) { fn(task_range_, states...); }, states_);
    BUG_ON(!task_range_.empty());

    return std::move(l_key);

  } else {
    RetT ret;
    std::apply([&](auto &...states) { ret = fn(task_range_, states...); },
               states_);
    BUG_ON(!task_range_.empty());

    return std::make_pair(std::move(l_key), std::move(ret));
  }
}

template <TaskRangeBased TR, typename... States>
template <typename RetT>
inline std::optional<compute_proclet_result<TR, RetT>>
ComputeProclet<TR, States...>::steal_and_compute(
    WeakProclet<ComputeProclet> victim, RetT (*fn)(TR &, States...)) {
  auto task_range = victim.run(&ComputeProclet::steal_tasks);
  return compute(fn, std::move(task_range));
}

template <TaskRangeBased TR, typename... States>
inline TR ComputeProclet<TR, States...>::steal_tasks() {
  ScopedLock g(&mutex_);

  auto lazy = task_range_.steal();
  g.reset();

  return std::move(lazy.get());
}

template <TaskRangeBased TR, typename... States>
void ComputeProclet<TR, States...>::suspend() {
  ScopedLock g(&mutex_);

  task_range_.suspend();
}

template <TaskRangeBased TR, typename... States>
void ComputeProclet<TR, States...>::resume() {
  ScopedLock g(&mutex_);

  task_range_.resume();
}

template <TaskRangeBased TR, typename... States>
inline std::size_t ComputeProclet<TR, States...>::remaining_size() {
  return task_range_.size();
}

template <TaskRangeBased TR, typename... States>
inline std::size_t ComputeProclet<TR, States...>::processed_size() {
  ScopedLock g(&mutex_);

  return task_range_.processed_size();
}

}  // namespace nu
