#include <algorithm>
#include <ranges>

namespace nu {

template <typename RetT, TaskRangeBased TR, typename... States>
DistributedExecutor<RetT, TR, States...>::DistributedExecutor()
    : almost_done_(false) {}

template <typename RetT, TaskRangeBased TR, typename... States>
std::vector<RetT> &&DistributedExecutor<RetT, TR, States...>::get() {
  return std::move(future_.get());
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
void DistributedExecutor<RetT, TR, States...>::spawn_initial_workers(
    S1s &... states) {
  add_workers(states...);

  if (unlikely(workers_.empty())) {
    workers_.emplace_back(
        nu::make_proclet<ComputeProclet<TR, States...>>(states...));
  }
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
void DistributedExecutor<RetT, TR, States...>::add_workers(S1s &... states) {
  if (unlikely(almost_done_)) {
    return;
  }

  std::vector<std::pair<NodeIP, Resource>> global_free_resources;
  {
    Caladan::PreemptGuard g;
    global_free_resources =
        get_runtime()->resource_reporter()->get_global_free_resources();
  }

  std::vector<Future<Proclet<ComputeProclet<TR, States...>>>> worker_futures;
  for (auto &[ip, resource] : global_free_resources) {
    for (uint32_t i = 0; i < resource.cores; i++) {
      worker_futures.emplace_back(
          nu::make_proclet_async<ComputeProclet<TR, States...>>(states...));
    }
  }

  std::ranges::transform(worker_futures, std::back_inserter(workers_),
                         [](auto &worker_future) {
                           return Worker{std::move(worker_future.get())};
                         });
}

template <typename RetT, TaskRangeBased TR, typename... States>
void DistributedExecutor<RetT, TR, States...>::make_initial_dispatch(
    RetT (*fn)(TR &, States...), TR task_range) {
  auto cmp = [](const TR &a, const TR &b) { return a.size() < b.size(); };
  std::priority_queue<TR, std::vector<TR>, decltype(cmp)> q(cmp);

  q.emplace(std::move(task_range));
  while (q.size() != workers_.size()) {
    auto biggest_tr = std::move(q.top());
    q.pop();
    auto new_tr = biggest_tr.split(biggest_tr.size() / 2);
    q.emplace(std::move(biggest_tr));
    q.emplace(std::move(new_tr));
  }

  for (auto &worker : workers_) {
    auto tr = std::move(q.top());
    q.pop();
    worker.future = worker.cp.__run_async(
        &ComputeProclet<TR, States...>::template compute<RetT>, fn,
        std::move(tr));
  }
}

template <typename RetT, TaskRangeBased TR, typename... States>
void DistributedExecutor<RetT, TR, States...>::check_workers() {
  victims_ = decltype(victims_)();
  auto futures = workers_ | std::views::transform([](auto &worker) {
                   return worker.cp.run_async(
                       &ComputeProclet<TR, States...>::remaining_size);
                 });
  for (auto t : std::views::zip(workers_, futures)) {
    auto &worker = std::get<0>(t);
    auto size = std::get<1>(t).get();
    worker.remaining_size = size;
    if (size) {
      victims_.push(&worker);
    } else {
      almost_done_ = true;
    }
  }
}

template <typename RetT, TaskRangeBased TR, typename... States>
std::vector<RetT> DistributedExecutor<RetT, TR, States...>::concat_results() {
  std::ranges::sort(all_pairs_,
                    [](auto &a, auto &b) { return a.first < b.first; });
  std::vector<RetT> all_rets;
  all_rets.reserve(all_pairs_.size());
  for (auto &[_, ret] : all_pairs_) {
    all_rets.emplace_back(std::move(ret));
  }
  return all_rets;
}

template <typename RetT, TaskRangeBased TR, typename... States>
bool DistributedExecutor<RetT, TR, States...>::check_futures_and_redispatch() {
  bool has_pending = false;

  for (auto &worker : workers_) {
    auto &future = worker.future;
    if (future && !future.is_ready()) {
      has_pending = true;
    } else {
      if (future) {
        all_pairs_.emplace_back(std::move(future.get()));
        auto gc = std::move(future);
      }
      if (!victims_.empty()) {
        auto *victim = victims_.top();
        victims_.pop();
        future = worker.cp.__run_async(
            &ComputeProclet<TR, States...>::template steal_and_compute<RetT>,
            victim->cp.get_weak(), fn_);
        has_pending = true;
      }
    }
  }

  return has_pending;
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
std::vector<RetT> DistributedExecutor<RetT, TR, States...>::run(
    RetT (*fn)(TR &, States...), TR task_range, S1s &&... states) {
  fn_ = fn;
  spawn_initial_workers(states...);
  make_initial_dispatch(fn, std::move(task_range));

  uint64_t last_check_workers_us = Time::microtime();
  uint64_t last_add_workers_us = last_check_workers_us;
  while (true) {
    auto now_us = Time::microtime();
    if (now_us - last_check_workers_us >= kCheckWorkersIntervalUs) {
      last_check_workers_us = now_us;
      check_workers();

      if (now_us - last_add_workers_us >= kAddWorkersIntervalUs) {
        last_add_workers_us = now_us;
        add_workers(states...);
      }
    }

    if (unlikely(!check_futures_and_redispatch())) {
      break;
    }

    rt::Yield();
  }

  return concat_results();
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
void DistributedExecutor<RetT, TR, States...>::start_async(
    RetT (*fn)(TR &, States...), TR task_range, S1s &&... states) {
  future_ = nu::async([&, fn, task_range = std::move(task_range),
                       ... states = std::forward<S1s>(states)]() mutable {
    return run(fn, std::move(task_range), std::forward<S1s>(states)...);
  });
}

template <typename RetT, TaskRangeBased TR, typename... S0s, typename... S1s>
DistributedExecutor<RetT, TR, S0s...> make_distributed_executor(
    RetT (*fn)(TR &, S0s...), TR task_range, S1s &&... states) {
  DistributedExecutor<RetT, TR, S0s...> dis_exec;
  dis_exec.start_async(fn, std::move(task_range), std::forward<S1s>(states)...);
  return dis_exec;
}
}
