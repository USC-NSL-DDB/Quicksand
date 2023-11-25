#include <algorithm>
#include <cstdint>
#include <ranges>
#include <syncstream>

#include "nu/utils/caladan.hpp"
#include "nu/utils/time.hpp"

namespace nu {

constexpr static bool kEnableLogging = false;
constexpr static auto kStaticQueueWorkerTarget = 0;

template <typename RetT, TaskRangeBased TR, typename... States>
DistributedExecutor<RetT, TR, States...>::Worker::Worker() {}

template <typename RetT, TaskRangeBased TR, typename... States>
DistributedExecutor<RetT, TR, States...>::Worker::Worker(
    Proclet<ComputeProclet<TR, States...>> cp)
    : cp(std::move(cp)) {}

template <typename RetT, TaskRangeBased TR, typename... States>
void DistributedExecutor<RetT, TR, States...>::Worker::compute_async(
    RetT (*fn)(TR &, States...), TR tr) {
  compute_future =
      cp.template __run_async</* MigrEn = */ true, /* CPUMon = */ true,
                              /* CPUSample = */ false>(
          &ComputeProclet<TR, States...>::template compute<RetT>, fn,
          std::move(tr));
}

template <typename RetT, TaskRangeBased TR, typename... States>
void DistributedExecutor<RetT, TR, States...>::Worker::steal_and_compute_async(
    WeakProclet<ComputeProclet<TR, States...>> victim,
    RetT (*fn)(TR &, States...)) {
  compute_future =
      cp.template __run_async</* MigrEn = */ true, /* CPUMon = */ true,
                              /* CPUSample = */ false>(
          &ComputeProclet<TR, States...>::template steal_and_compute<RetT>,
          victim, fn);
}

template <typename RetT, TaskRangeBased TR, typename... States>
void DistributedExecutor<RetT, TR, States...>::Worker::update_remaining_size() {
  remaining_size = cp.run(&ComputeProclet<TR, States...>::remaining_size);
}

template <typename RetT, TaskRangeBased TR, typename... States>
void DistributedExecutor<RetT, TR, States...>::Worker::suspend() {
  cp.run(&ComputeProclet<TR, States...>::suspend);
}

template <typename RetT, TaskRangeBased TR, typename... States>
void DistributedExecutor<
    RetT, TR, States...>::Worker::resume_and_update_remaining_size() {
  remaining_size = cp.run(+[](ComputeProclet<TR, States...> &cp) {
    cp.resume();
    return cp.remaining_size();
  });
}

template <typename RetT, TaskRangeBased TR, typename... States>
DistributedExecutor<RetT, TR, States...>::DistributedExecutor() {}

template <typename RetT, TaskRangeBased TR, typename... States>
DistributedExecutor<RetT, TR, States...>::~DistributedExecutor() {
  std::vector<nu::Future<void>> futures;

  auto fn = [](auto &worker_unique_ptr) {
    return nu::async([&worker_unique_ptr] { worker_unique_ptr.reset(); });
  };
  std::ranges::transform(active_workers_, std::back_inserter(futures), fn);
  std::ranges::transform(suspended_workers_, std::back_inserter(futures), fn);
}

template <typename RetT, TaskRangeBased TR, typename... States>
DistributedExecutor<RetT, TR, States...>::MovedResult
DistributedExecutor<RetT, TR, States...>::get() {
  if constexpr (std::is_void_v<MovedResult>) {
    future_.get();
  } else {
    return std::move(future_.get());
  }
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
void DistributedExecutor<RetT, TR, States...>::spawn_initial_workers(
    S1s &...states) {
  add_workers(states...);
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
void DistributedExecutor<RetT, TR, States...>::add_workers(S1s &...states) {
  std::vector<std::pair<NodeIP, Resource>> global_free_resources;
  {
    Caladan::PreemptGuard g;
    global_free_resources =
        get_runtime()->resource_reporter()->get_global_free_resources();
  }

  uint64_t num_new_workers = 0;
  for (auto &[_, resource] : global_free_resources) {
    num_new_workers += resource.cores;
  }
  num_new_workers = std::min(num_new_workers, victims_.size());

  std::vector<Future<Proclet<ComputeProclet<TR, States...>>>> futures;
  for (uint64_t i = 0; i < num_new_workers; i++) {
    futures.emplace_back(nu::make_proclet_async<ComputeProclet<TR, States...>>(
        std::forward_as_tuple(states...)));
  }

  for (auto &future : futures) {
    active_workers_.emplace_back(
        std::make_unique<Worker>(std::move(future.get())));
  }
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
void DistributedExecutor<RetT, TR, States...>::make_initial_dispatch(
    RetT (*fn)(TR &, States...), TR task_range, S1s &&...states) {
  // We need at least one worker to get started.
  if (unlikely(active_workers_.empty())) {
    active_workers_.emplace_back(std::make_unique<Worker>(
        nu::make_proclet<ComputeProclet<TR, States...>>(
            std::forward_as_tuple(states...))));
  }

  auto cmp = [](const TR &a, const TR &b) { return a.size() < b.size(); };
  std::priority_queue<TR, std::vector<TR>, decltype(cmp)> q(cmp);

  q.emplace(std::move(task_range));
  while (q.size() != active_workers_.size()) {
    auto biggest_tr = std::move(q.top());
    q.pop();
    auto new_tr = biggest_tr.steal().get();
    q.emplace(std::move(biggest_tr));
    q.emplace(std::move(new_tr));
  }

  for (auto &worker : active_workers_) {
    auto tr = std::move(q.top());
    q.pop();
    if (!tr.empty()) {
      worker->compute_async(fn, std::move(tr));
    }
  }
}

template <typename RetT, TaskRangeBased TR, typename... States>
void DistributedExecutor<RetT, TR, States...>::check_workers() {
  std::vector<std::pair<Worker *, nu::Future<void>>> updaters;

  auto fn = [](auto &worker_unique_ptr) {
    auto *worker_ptr = worker_unique_ptr.get();
    return std::make_pair(
        worker_ptr, nu::async([=] { worker_ptr->update_remaining_size(); }));
  };
  std::ranges::transform(active_workers_, std::back_inserter(updaters), fn);
  std::ranges::transform(suspended_workers_, std::back_inserter(updaters), fn);

  victims_ = decltype(victims_)();
  for (auto &[worker_ptr, future] : updaters) {
    future.get();
    if (worker_ptr->remaining_size) {
      victims_.push(worker_ptr);
    }
  }
}

template <typename RetT, TaskRangeBased TR, typename... States>
DistributedExecutor<RetT, TR, States...>::Result
DistributedExecutor<RetT, TR, States...>::concat_results() {
  if constexpr (std::is_same_v<Result, void>) {
    return;
  } else {
    std::ranges::sort(all_pairs_,
                      [](auto &a, auto &b) { return a.first < b.first; });
    std::vector<RetT> all_rets;
    all_rets.reserve(all_pairs_.size());
    for (auto &[_, ret] : all_pairs_) {
      all_rets.emplace_back(std::move(ret));
    }
    return all_rets;
  }
}

template <typename RetT, TaskRangeBased TR, typename... States>
bool DistributedExecutor<RetT, TR, States...>::check_futures_and_redispatch() {
  bool has_pending = false;

  for (auto &worker_ptr : active_workers_) {
    auto &future = worker_ptr->compute_future;
    if (future) {
      if (!future.is_ready()) {
        has_pending = true;
        continue;
      } else {
        auto &optional_ret = future.get();
        if (optional_ret) {
          all_pairs_.emplace_back(std::move(*optional_ret));
        }
        auto gc = std::move(future);  // Release mem ASAP.
      }
    }

    if (!victims_.empty()) {
      // Steal from victims.
      auto *victim = victims_.top();
      victims_.pop();
      victim->remaining_size /= 2;
      if (victim->remaining_size) {
        victims_.push(victim);
      }
      worker_ptr->steal_and_compute_async(victim->cp.get_weak(), fn_);
      has_pending = true;
    }
  }

  return has_pending;
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
DistributedExecutor<RetT, TR, States...>::Result
DistributedExecutor<RetT, TR, States...>::run(RetT (*fn)(TR &, States...),
                                              TR task_range, S1s &&...states) {
  fn_ = fn;
  spawn_initial_workers(states...);
  make_initial_dispatch(fn, std::move(task_range), states...);

  uint64_t last_check_workers_us = Time::microtime();
  uint64_t last_add_workers_us = last_check_workers_us;
  while (true) {
    auto now_us = Time::microtime();

    if (now_us - last_check_workers_us >= kCheckWorkersIntervalUs) {
      check_workers();
      last_check_workers_us = now_us = Time::microtime();
    }
    auto sleep_us = kCheckWorkersIntervalUs - (now_us - last_check_workers_us);

    if (now_us - last_add_workers_us >= kAdjustNumWorkersIntervalUs) {
      add_workers(states...);

      if constexpr (kEnableLogging) {
        Caladan::PreemptGuard g;

        std::osyncstream synced_out(std::cout);
        synced_out << microtime() << " " << active_workers_.size() << std::endl;
      }

      last_add_workers_us = now_us = Time::microtime();
    }
    sleep_us = std::min(
        sleep_us, kAdjustNumWorkersIntervalUs - (now_us - last_add_workers_us));

    if (unlikely(!check_futures_and_redispatch())) {
      break;
    }

    Time::sleep(sleep_us);
  }

  return concat_results();
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
void DistributedExecutor<RetT, TR, States...>::start_async(
    RetT (*fn)(TR &, States...), TR task_range, S1s &&...states) {
  future_ = nu::async([&, fn, task_range = std::move(task_range),
                       ... states = std::forward<S1s>(states)]() mutable {
    return run(fn, std::move(task_range), std::forward<S1s>(states)...);
  });
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
void DistributedExecutor<RetT, TR, States...>::spawn_initial_queue_workers(
    TR task_range, S1s &...states) {
  std::vector<std::pair<NodeIP, Resource>> global_free_resources;
  {
    Caladan::PreemptGuard g;
    global_free_resources =
        get_runtime()->resource_reporter()->get_global_free_resources();
  }

  std::vector<Future<Proclet<ComputeProclet<TR, States...>>>> futures;
  for (auto &[ip, resource] : global_free_resources) {
    auto num_workers = static_cast<uint32_t>(resource.cores);
    for (uint32_t i = 0; i < num_workers; i++) {
      futures.emplace_back(
          nu::make_proclet_async<ComputeProclet<TR, States...>>(
              std::forward_as_tuple(states...), false, std::nullopt, ip));
    }
  }

  std::ranges::transform(
      futures, std::back_inserter(active_workers_), [](auto &future) {
        return std::make_unique<Worker>(std::move(future.get()));
      });
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
void DistributedExecutor<RetT, TR, States...>::adjust_num_active_queue_workers(
    int delta, S1s &...states) {
  std::vector<std::pair<NodeIP, Resource>> global_free_resources;
  {
    Caladan::PreemptGuard g;
    global_free_resources =
        get_runtime()->resource_reporter()->get_global_free_resources();
  }

  std::size_t target;
  if constexpr (kStaticQueueWorkerTarget) {
    target = kStaticQueueWorkerTarget;
  } else {
    int total_num_idle_cores = 0;
    for (auto &[ip, res] : global_free_resources) {
      total_num_idle_cores += res.cores;
    }

    delta = std::min(delta, total_num_idle_cores);
    target = static_cast<std::size_t>(
        std::max(1, delta + static_cast<int>(active_workers_.size())));
  }

  if (target < active_workers_.size()) {
    // We should suspend workers.
    std::vector<nu::Future<void>> futures;
    while (active_workers_.size() != target) {
      suspended_workers_.emplace_back(std::move(active_workers_.back()));
      futures.emplace_back(
          nu::async([worker_ptr = suspended_workers_.back().get()] {
            worker_ptr->suspend();
          }));
      active_workers_.pop_back();
    }
  } else {
    // We should add workers.
    if (!suspended_workers_.empty()) {
      std::vector<nu::Future<void>> futures;
      // Try to reuse the suspended workers.
      while (active_workers_.size() != target && !suspended_workers_.empty()) {
        active_workers_.emplace_back(std::move(suspended_workers_.back()));
        futures.emplace_back(
            nu::async([worker_ptr = active_workers_.back().get()] {
              worker_ptr->resume_and_update_remaining_size();
            }));
        suspended_workers_.pop_back();
      }
    } else {
      // Create more workers.
      std::vector<nu::Future<Proclet<ComputeProclet<TR, States...>>>> futures;
      for (auto &[ip, resource] : global_free_resources) {
        auto num = std::min(delta, static_cast<int>(resource.cores));
        for (int i = 0; i < num; i++) {
          futures.emplace_back(
              nu::make_proclet_async<ComputeProclet<TR, States...>>(
                  std::forward_as_tuple(states...), false, std::nullopt, ip));
        }
        delta -= num;
      }

      for (auto &future : futures) {
        active_workers_.emplace_back(
            std::make_unique<Worker>(std::move(future.get())));
      }
    }
  }
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <ShardedQueueBased Q, typename... S1s>
DistributedExecutor<RetT, TR, States...>::Result
DistributedExecutor<RetT, TR, States...>::run_queue(RetT (*fn)(TR &, States...),
                                                    TR task_range, Q queue,
                                                    S1s &&...states) {
  fn_ = fn;

  constexpr auto kSlowStartQueueLen = 100;
  constexpr auto kSlowStartStep = 2;
  constexpr auto kDiffQueueLenMultiplier = -0.5;

  uint64_t last_check_workers_us = microtime();
  uint64_t last_adjust_num_workers_us = last_check_workers_us;
  int64_t prev_queue_len = 0;

  spawn_initial_queue_workers(task_range, states...);
  make_initial_dispatch(fn_, std::move(task_range), states...);
  while (true) {
    auto now_us = Time::microtime();

    if (now_us - last_check_workers_us >= kCheckWorkersIntervalUs) {
      check_workers();
      last_check_workers_us = now_us = Time::microtime();
    }
    auto sleep_us = kCheckWorkersIntervalUs - (now_us - last_check_workers_us);

    if (now_us - last_adjust_num_workers_us >= kAdjustNumWorkersIntervalUs) {
      int64_t curr_queue_len = queue.size();
      auto diff_queue_len = curr_queue_len - prev_queue_len;
      auto delta =
          curr_queue_len < kSlowStartQueueLen
              ? kSlowStartStep
              : static_cast<int>(kDiffQueueLenMultiplier * diff_queue_len);
      adjust_num_active_queue_workers(delta, states...);

      if constexpr (kEnableLogging) {
        Caladan::PreemptGuard g;

        std::osyncstream synced_out(std::cout);
        synced_out << microtime() << " " << curr_queue_len << " "
                   << diff_queue_len << " " << delta << " "
                   << active_workers_.size() << " " << suspended_workers_.size()
                   << std::endl;
      }

      prev_queue_len = curr_queue_len;
      last_adjust_num_workers_us = now_us = Time::microtime();
    }
    sleep_us = std::min(sleep_us, kAdjustNumWorkersIntervalUs -
                                      (now_us - last_adjust_num_workers_us));

    if (unlikely(!check_futures_and_redispatch())) {
      break;
    }

    Time::sleep(sleep_us);
  }

  active_workers_.clear(); // Wait for the inflight computation to finish.
  return concat_results();
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <ShardedQueueBased Q, typename... S1s>
void DistributedExecutor<RetT, TR, States...>::start_queue_async(
    RetT (*fn)(TR &, States...), TR task_range, Q queue, S1s &&...states) {
  future_ = nu::async([&, fn, task_range = std::move(task_range),
                       queue = std::move(queue),
                       ... states = std::forward<S1s>(states)]() mutable {
    return run_queue<Q, S1s...>(fn, std::move(task_range), std::move(queue),
                                std::forward<S1s>(states)...);
  });
}

template <typename RetT, TaskRangeBased TR, typename... S0s, typename... S1s>
DistributedExecutor<RetT, TR, S0s...> make_distributed_executor(
    RetT (*fn)(TR &, S0s...), TR task_range, S1s &&...states) {
  DistributedExecutor<RetT, TR, S0s...> dis_exec;
  dis_exec.start_async(fn, std::move(task_range), std::forward<S1s>(states)...);
  return dis_exec;
}

template <typename RetT, TaskRangeBased TR, ShardedQueueBased Q,
          typename... S0s, typename... S1s>
DistributedExecutor<RetT, TR, Q, S0s...> make_distributed_executor(
    RetT (*fn)(TR &, Q, S0s...), TR task_range, Q queue, S1s &&...states) {
  DistributedExecutor<RetT, TR, Q, S0s...> dis_exec;
  Q queue_cp_arg = queue;
  dis_exec.template start_queue_async<Q, Q, S1s...>(
      fn, std::move(task_range), std::move(queue), std::move(queue_cp_arg),
      std::forward<S1s>(states)...);
  return dis_exec;
}

}  // namespace nu
