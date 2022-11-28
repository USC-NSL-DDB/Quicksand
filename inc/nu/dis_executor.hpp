#pragma once

#include <queue>
#include <vector>

#include "nu/compute_proclet.hpp"
#include "nu/task_range.hpp"
#include "nu/utils/future.hpp"

namespace nu {

template <typename RetT, TaskRangeBased TR, typename... States>
class DistributedExecutor {
 public:
  std::vector<RetT> &&get();
  DistributedExecutor(const DistributedExecutor &) = delete;
  DistributedExecutor &operator=(const DistributedExecutor &) = delete;
  DistributedExecutor(DistributedExecutor &&) = default;
  DistributedExecutor &operator=(DistributedExecutor &&) = default;

 private:
  struct Worker {
    Proclet<ComputeProclet<TR, States...>> cp;
    Future<std::pair<typename TR::Key, std::vector<RetT>>> future;
    std::size_t remaining_size;
  };
  struct VictimCmp {
    bool operator()(const Worker *a, const Worker *b) const {
      return a->remaining_size < b->remaining_size;
    }
  };

  constexpr static uint64_t kCheckWorkerIntervalUs = 200;
  RetT (*fn_)(TR &, States...);
  std::vector<Worker> workers_;
  Future<std::vector<RetT>> future_;
  std::priority_queue<Worker *, std::vector<Worker *>, VictimCmp> victims_;
  std::vector<std::pair<typename TR::Key, std::vector<RetT>>> all_pairs_;
  template <typename R, TaskRangeBased T, typename... S0s, typename... S1s>
  friend DistributedExecutor<R, T, S0s...> make_distributed_executor(
      R (*fn)(T &, S0s...), T, S1s &&...);

  DistributedExecutor() = default;
  template <typename... S1s>
  std::vector<RetT> run(RetT (*fn)(TR &, States...), TR task_range,
                        S1s &&... states);
  template <typename... S1s>
  void start_async(RetT (*fn)(TR &, States...), TR task_range,
                   S1s &&... states);
  template <typename... S1s>
  void spawn_initial_workers(S1s &... states);
  void make_initial_dispatch(RetT (*fn)(TR &, States...), TR task_range);
  void check_worker();
  std::vector<RetT> concat_results();
  bool check_futures_and_redispatch();
};

template <typename RetT, TaskRangeBased TR, typename... S0s, typename... S1s>
DistributedExecutor<RetT, TR, S0s...> make_distributed_executor(
    RetT (*fn)(TR &, S0s...), TR task_range, S1s &&... states);

}  // namespace nu

#include "nu/impl/dis_executor.ipp"
