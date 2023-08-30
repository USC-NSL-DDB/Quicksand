#include <algorithm>
#include <cstdint>
#include <ranges>
#include <syncstream>

#include "nu/utils/caladan.hpp"
#include "nu/utils/time.hpp"

namespace nu {

constexpr static bool kEnableLogging = false;

template <typename RetT, TaskRangeBased TR, typename... States>
DistributedExecutor<RetT, TR, States...>::Worker::Worker() {}

template <typename RetT, TaskRangeBased TR, typename... States>
DistributedExecutor<RetT, TR, States...>::Worker::Worker(
    Proclet<ComputeProclet<TR, States...>> cp)
    : cp(std::move(cp)), spawned_time(Time::microtime()) {}

template <typename RetT, TaskRangeBased TR, typename... States>
void DistributedExecutor<RetT, TR, States...>::Worker::compute_async(
    RetT (*fn)(TR &, States...), TR tr) {
  future =
      cp.__run_async(&ComputeProclet<TR, States...>::template compute<RetT>, fn,
                     std::move(tr));
}

template <typename RetT, TaskRangeBased TR, typename... States>
void DistributedExecutor<RetT, TR, States...>::Worker::steal_and_compute_async(
    WeakProclet<ComputeProclet<TR, States...>> victim,
    RetT (*fn)(TR &, States...)) {
  future = cp.__run_async(
      &ComputeProclet<TR, States...>::template steal_and_compute<RetT>, victim,
      fn);
}

template <typename RetT, TaskRangeBased TR, typename... States>
DistributedExecutor<RetT, TR, States...>::DistributedExecutor() {}

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
    S1s &... states) {
  add_workers(states...);

  if (unlikely(workers_.empty())) {
    workers_.emplace_back(nu::make_proclet<ComputeProclet<TR, States...>>(
        std::forward_as_tuple(states...)));
  }
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
void DistributedExecutor<RetT, TR, States...>::add_workers(S1s &... states) {
  std::vector<std::pair<NodeIP, Resource>> global_free_resources;
  {
    Caladan::PreemptGuard g;
    global_free_resources =
        get_runtime()->resource_reporter()->get_global_free_resources();
  }

  std::vector<Future<Proclet<ComputeProclet<TR, States...>>>> worker_futures;
  for (auto &[ip, resource] : global_free_resources) {
    auto num_workers = static_cast<uint32_t>(resource.cores);
    for (uint32_t i = 0; i < num_workers; i++) {
      worker_futures.emplace_back(
          nu::make_proclet_async<ComputeProclet<TR, States...>>(
              std::forward_as_tuple(states...), false, std::nullopt, ip));
    }
  }

  std::ranges::transform(worker_futures, std::back_inserter(workers_),
                         [](auto &worker_future) {
                           return Worker(std::move(worker_future.get()));
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
    if (!tr.empty()) {
      worker.compute_async(fn, std::move(tr));
    }
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

  for (auto &worker : workers_) {
    auto &future = worker.future;
    if (future && !future.is_ready()) {
      has_pending = true;
    } else {
      if (future) {
        auto &optional_ret = future.get();
        if (optional_ret) {
          all_pairs_.emplace_back(std::move(*optional_ret));
        }
        auto gc = std::move(future);
      }
      if (!victims_.empty()) {
        auto *victim = victims_.top();
        victims_.pop();
        worker.steal_and_compute_async(victim->cp.get_weak(), fn_);
        has_pending = true;
      }
    }
  }

  return has_pending;
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
DistributedExecutor<RetT, TR, States...>::Result
DistributedExecutor<RetT, TR, States...>::run(RetT (*fn)(TR &, States...),
                                              TR task_range, S1s &&... states) {
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
    }
    auto sleep_us = kCheckWorkersIntervalUs - (now_us - last_check_workers_us);

    if (now_us - last_add_workers_us >= kAdjustNumWorkersIntervalUs) {
      last_add_workers_us = now_us;
      add_workers(states...);

      if constexpr (kEnableLogging) {
        Caladan::PreemptGuard g;

        std::osyncstream synced_out(std::cout);
        synced_out << microtime() << " " << workers_.size() << std::endl;
      }
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
    RetT (*fn)(TR &, States...), TR task_range, S1s &&... states) {
  future_ = nu::async([&, fn, task_range = std::move(task_range),
                       ... states = std::forward<S1s>(states)]() mutable {
    return run(fn, std::move(task_range), std::forward<S1s>(states)...);
  });
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
void DistributedExecutor<RetT, TR, States...>::spawn_initial_queue_workers(
    TR task_range, S1s &... states) {
  std::vector<std::pair<NodeIP, Resource>> global_free_resources;
  {
    Caladan::PreemptGuard g;
    global_free_resources =
        get_runtime()->resource_reporter()->get_global_free_resources();
  }

  std::vector<Future<Proclet<ComputeProclet<TR, States...>>>> worker_futures;
  for (auto &[ip, resource] : global_free_resources) {
    auto num_workers = static_cast<uint32_t>(resource.cores);
    for (uint32_t i = 0; i < num_workers; i++) {
      worker_futures.emplace_back(
          nu::make_proclet_async<ComputeProclet<TR, States...>>(
              std::forward_as_tuple(states...), false, std::nullopt, ip));
    }
  }

  std::ranges::transform(worker_futures, std::back_inserter(workers_),
                         [](auto &worker_future) {
                           return Worker(std::move(worker_future.get()));
                         });
  num_active_workers_ = workers_.size();
  make_initial_dispatch(fn_, std::move(task_range));
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
void DistributedExecutor<RetT, TR, States...>::adjust_queue_workers(
    std::size_t target, TR task_range, S1s &... states) {
  target = std::max(target, static_cast<std::size_t>(1));

  if (target < num_active_workers_) {
    auto futures = std::vector<nu::Future<void>>{};
    for (std::size_t i = target; i < num_active_workers_; i++) {
      futures.push_back(
          workers_[i].cp.run_async(&ComputeProclet<TR, States...>::suspend));
    }
    for (auto &f : futures) {
      f.get();
    }
    num_active_workers_ = target;
  } else if (target > num_active_workers_) {
    if (num_active_workers_ < workers_.size()) {
      std::size_t scale_up_target = std::min(workers_.size(), target);

      auto futures = std::vector<nu::Future<void>>{};
      for (std::size_t i = num_active_workers_; i < scale_up_target; i++) {
        futures.push_back(std::move(
            workers_[i].cp.run_async(&ComputeProclet<TR, States...>::resume)));
      }

      for (auto &f : futures) {
        f.get();
      }
      num_active_workers_ = scale_up_target;
    }
    if (target > num_active_workers_) {
      std::vector<std::pair<NodeIP, Resource>> global_free_resources;
      {
        Caladan::PreemptGuard g;
        global_free_resources =
            get_runtime()->resource_reporter()->get_global_free_resources();
      }

      std::size_t gap = target - workers_.size();
      std::vector<Future<Proclet<ComputeProclet<TR, States...>>>>
          worker_futures;
      for (auto &[ip, resource] : global_free_resources) {
        auto num_workers =
            std::min(gap, static_cast<std::size_t>(resource.cores));
        gap -= num_workers;
        for (uint32_t i = 0; i < num_workers; i++) {
          worker_futures.emplace_back(
              nu::make_proclet_async<ComputeProclet<TR, States...>>(
                  std::forward_as_tuple(states...), false, std::nullopt, ip));
        }
      }

      for (auto &f : worker_futures) {
        auto w = Worker(std::move(f.get()));
        workers_.push_back(std::move(w));
      }
      num_active_workers_ = workers_.size();
    }
  }
}

template <typename RetT, TaskRangeBased TR, typename... States>
void DistributedExecutor<RetT, TR, States...>::abort_workers() {
  victims_ = decltype(victims_)();
  auto futures = std::vector<nu::Future<void>>{};
  for (auto &w : workers_) {
    futures.push_back(
        std::move(w.cp.run_async(&ComputeProclet<TR, States...>::abort)));
  }
  for (auto &f : futures) {
    f.get();
  }
}

template <typename RetT, TaskRangeBased TR, typename... States>
float DistributedExecutor<RetT, TR, States...>::check_queue_workers() {
  victims_ = decltype(victims_)();
  auto processed_sizes = workers_ | std::views::transform([](auto &worker) {
                           return worker.cp.run_async(
                               &ComputeProclet<TR, States...>::processed_size);
                         });
  float total_tp_ms = 0;
  auto time_now = Time::microtime();
  for (auto t : std::views::zip(workers_, processed_sizes)) {
    auto &worker = std::get<0>(t);
    auto size = std::get<1>(t).get();
    worker.processed_size = size;

    float tp_ms = static_cast<float>(1000 * size) /
                  static_cast<float>(time_now - worker.spawned_time);
    total_tp_ms += tp_ms;
  }
  float avg_tp_ms = total_tp_ms / workers_.size();
  return avg_tp_ms;
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <ShardedQueueBased Q, typename... S1s>
DistributedExecutor<RetT, TR, States...>::Result
DistributedExecutor<RetT, TR, States...>::run_queue(RetT (*fn)(TR &, States...),
                                                    TR task_range, Q queue,
                                                    S1s &&...states) {
  fn_ = fn;

  constexpr auto kSlowStartQueueLen = 100;
  constexpr auto kSlowStartStep = 10;
  constexpr auto kDiffQueueLenMultiplier = -0.4;

  uint64_t last_check_workers_us = microtime();
  uint64_t last_adjust_num_workers_us = last_check_workers_us;
  int64_t prev_queue_len = 0;

  spawn_initial_queue_workers(task_range, states...);

  while (true) {
    auto now_us = Time::microtime();

    if (now_us - last_check_workers_us >= kCheckWorkersIntervalUs) {
      check_workers();
      last_check_workers_us = now_us;
    }
    auto sleep_us = kCheckWorkersIntervalUs - (now_us - last_check_workers_us);

    if (now_us - last_adjust_num_workers_us >= kAdjustNumWorkersIntervalUs) {
      int64_t curr_queue_len = queue.size();
      auto diff_queue_len = curr_queue_len - prev_queue_len;
      auto delta_num_active_workers =
          curr_queue_len < kSlowStartQueueLen
              ? kSlowStartStep
              : kDiffQueueLenMultiplier * diff_queue_len;
      auto new_num_active_workers = std::max(
          0, static_cast<int>(num_active_workers_ + delta_num_active_workers));
      adjust_queue_workers(new_num_active_workers, task_range, states...);

      if constexpr (kEnableLogging) {
        Caladan::PreemptGuard g;

        std::osyncstream synced_out(std::cout);
        synced_out << microtime() << " " << curr_queue_len << " "
                   << new_num_active_workers << std::endl;
      }

      prev_queue_len = curr_queue_len;
      last_adjust_num_workers_us = now_us;
    }
    sleep_us = std::min(sleep_us, kAdjustNumWorkersIntervalUs -
                                      (now_us - last_adjust_num_workers_us));

    if (unlikely(!check_futures_and_redispatch())) {
      break;
    }

    Time::sleep(sleep_us);
  }

  while (queue.size()) {
    Time::sleep(kCheckWorkersIntervalUs);
  }

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
    RetT (*fn)(TR &, S0s...), TR task_range, S1s &&... states) {
  DistributedExecutor<RetT, TR, S0s...> dis_exec;
  dis_exec.start_async(fn, std::move(task_range), std::forward<S1s>(states)...);
  return dis_exec;
}

template <typename RetT, TaskRangeBased TR, ShardedQueueBased Q,
          typename... S0s, typename... S1s>
DistributedExecutor<RetT, TR, Q, S0s...> make_distributed_executor(
    RetT (*fn)(TR &, Q, S0s...), TR task_range, Q queue, S1s &&... states) {
  DistributedExecutor<RetT, TR, Q, S0s...> dis_exec;
  Q queue_cp_arg = queue;
  dis_exec.template start_queue_async<Q, Q, S1s...>(
      fn, std::move(task_range), std::move(queue), std::move(queue_cp_arg),
      std::forward<S1s>(states)...);
  return dis_exec;
}

}  // namespace nu
