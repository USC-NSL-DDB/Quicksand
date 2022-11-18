#include <type_traits>

#include "nu/proclet.hpp"

namespace nu {

template <class TR, typename... States>
ComputeProcletWorker<TR, States...>::ComputeProcletWorker(States... states)
    : states_(std::move(states)...) {}

template <class TR, typename... States>
template <typename RetT>
RetT ComputeProcletWorker<TR, States...>::compute(RetT (*fn)(TR &, States...),
                                                  TR task_range) {
  task_range_ = std::move(task_range);
  if constexpr (std::is_same_v<RetT, void>) {
    std::apply([&](auto &... states) { fn(task_range_, states...); }, states_);
  } else {
    return std::apply(
        [&](auto &... states) { return fn(task_range_, states...); }, states_);
  }
}

template <class TR>
template <typename RetT, typename... S0s, typename... S1s>
inline std::vector<RetT> ComputeProclet<TR>::run(RetT (*fn)(TR &, S0s...),
                                                 TR task_range,
                                                 S1s &&... states) {
  std::vector<Proclet<ComputeProcletWorker<TR, S0s...>>> workers;
  workers.emplace_back(
      nu::make_proclet<ComputeProcletWorker<TR, S0s...>>(states...));
  return std::vector<RetT>{workers.front().__run(
      &ComputeProcletWorker<TR, S0s...>::template compute<RetT>, fn,
      std::move(task_range))};
}

template <class TR>
template <typename RetT, typename... S0s, typename... S1s>
inline Future<std::vector<RetT>> ComputeProclet<TR>::run_async(
    RetT (*fn)(TR &, S0s...), TR task_range, S1s &&... states) {
  return nu::async([&, fn, task_range = std::move(task_range),
                    ... states = std::forward<S1s>(states)]() mutable {
    return run(fn, task_range, std::forward<S1s>(states)...);
  });
}

}  // namespace nu
