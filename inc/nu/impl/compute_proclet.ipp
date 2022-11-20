#include <algorithm>
#include <type_traits>

#include "nu/proclet.hpp"
#include "nu/utils/scoped_lock.hpp"

namespace nu {

template <class TR, typename... States>
inline ComputeProcletWorker<TR, States...>::ComputeProcletWorker(
    States... states)
    : states_(std::move(states)...) {}

template <class TR, typename... States>
template <typename RetT>
inline std::vector<RetT> ComputeProcletWorker<TR, States...>::compute(
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
inline TR ComputeProcletWorker<TR, States...>::steal_work() {
  ScopedLock g(&mutex_);
  return task_range_.split();
}

template <class TR>
template <typename RetT, typename... S0s, typename... S1s>
std::vector<RetT> ComputeProclet<TR>::run(RetT (*fn)(TR &, S0s...),
                                          TR task_range, S1s &&... states) {
  std::vector<Future<Proclet<ComputeProcletWorker<TR, S0s...>>>> workers;
  std::vector<Future<std::vector<RetT>>> futures;
  std::vector<RetT> rets;

  for (uint32_t i = 0; i < 2; i++) {
    workers.emplace_back(
        nu::make_proclet_async<ComputeProcletWorker<TR, S0s...>>(states...));
  }
  futures.emplace_back(workers[0].get().__run_async(
      &ComputeProcletWorker<TR, S0s...>::template compute<RetT>, fn,
      std::move(task_range)));
  delay_us(10 * 1000);
  auto stealed_tr =
      workers[0].get().run(&ComputeProcletWorker<TR, S0s...>::steal_work);
  futures.emplace_back(workers[1].get().__run_async(
      &ComputeProcletWorker<TR, S0s...>::template compute<RetT>, fn,
      std::move(stealed_tr)));
  for (auto &future : futures) {
    auto &future_val = future.get();
    rets.insert(rets.end(), std::make_move_iterator(future_val.begin()),
                std::make_move_iterator(future_val.end()));
  }
  return rets;
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
