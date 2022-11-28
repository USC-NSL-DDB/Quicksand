namespace nu {

template <typename RetT, TaskRangeBased TR, typename... States>
std::vector<RetT> &&DistributedExecutor<RetT, TR, States...>::get() {
  return std::move(future_.get());
}

template <typename RetT, TaskRangeBased TR, typename... States>
template <typename... S1s>
std::vector<RetT> DistributedExecutor<RetT, TR, States...>::run(
    RetT (*fn)(TR &, States...), TR task_range, S1s &&... states) {
  std::vector<Future<Proclet<ComputeProclet<TR, States...>>>> workers;
  std::map<typename TR::Key, Future<std::vector<RetT>>> futures;
  std::vector<RetT> rets;
  std::vector<std::pair<NodeIP, Resource>> global_free_resources;

  {
    Caladan::PreemptGuard g;
    global_free_resources =
        get_runtime()->resource_reporter()->get_global_free_resources();
  }
  for (auto &[ip, resource] : global_free_resources) {
    for (uint32_t i = 0; i < resource.cores; i++) {
      workers.emplace_back(
          nu::make_proclet_async<ComputeProclet<TR, States...>>(states...));
    }
  }
  if (unlikely(workers.empty())) {
    workers.emplace_back(
        nu::make_proclet_async<ComputeProclet<TR, States...>>(states...));
  }

  auto cmp = [](const TR &a, const TR &b) { return a.size() < b.size(); };
  std::priority_queue<TR, std::vector<TR>, decltype(cmp)> q(cmp);
  q.emplace(std::move(task_range));
  while (q.size() != workers.size()) {
    auto biggest_tr = std::move(q.top());
    q.pop();
    auto new_tr = biggest_tr.split();
    q.emplace(std::move(biggest_tr));
    q.emplace(std::move(new_tr));
  }

  for (auto &worker : workers) {
    auto tr = std::move(q.top());
    q.pop();
    auto l_key = tr.initial_key_range().first;
    futures.try_emplace(
        l_key, worker.get().__run_async(
                   &ComputeProclet<TR, States...>::template compute<RetT>, fn,
                   std::move(tr)));
  }

  for (auto &[_, future] : futures) {
    auto &future_val = future.get();
    rets.insert(rets.end(), std::make_move_iterator(future_val.begin()),
                std::make_move_iterator(future_val.end()));
  }

  return rets;
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
