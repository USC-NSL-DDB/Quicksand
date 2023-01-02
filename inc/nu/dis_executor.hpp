#pragma once

#include <deque>
#include <queue>
#include <type_traits>
#include <vector>

#include "nu/compute_proclet.hpp"
#include "nu/task_range.hpp"
#include "nu/utils/future.hpp"

namespace nu {

template <typename RetT, TaskRangeBased TR, typename... States>
class DistributedExecutor {
 public:
  using Result =
      std::conditional_t<std::is_void_v<RetT>, void, std::vector<RetT>>;
  using MovedResult =
      std::conditional_t<std::is_void_v<Result>, void, std::vector<RetT> &&>;

  DistributedExecutor(const DistributedExecutor &) = delete;
  DistributedExecutor &operator=(const DistributedExecutor &) = delete;
  DistributedExecutor(DistributedExecutor &&) = default;
  DistributedExecutor &operator=(DistributedExecutor &&) = default;

  MovedResult get();

 private:
  struct Worker {
    Proclet<ComputeProclet<TR, States...>> cp;
    Future<std::optional<compute_proclet_result<TR, RetT>>> future;
    std::size_t remaining_size;
  };
  struct VictimCmp {
    bool operator()(const Worker *a, const Worker *b) const {
      return a->remaining_size < b->remaining_size;
    }
  };

  constexpr static uint64_t kCheckWorkersIntervalUs = 200;
  constexpr static uint64_t kAddWorkersIntervalUs = 1000;
  RetT (*fn_)(TR &, States...);
  bool almost_done_;
  std::deque<Worker> workers_;
  Future<Result> future_;
  std::priority_queue<Worker *, std::vector<Worker *>, VictimCmp> victims_;
  std::vector<compute_proclet_result<TR, RetT>> all_pairs_;

  template <typename R, TaskRangeBased T, typename... S0s, typename... S1s>
  friend DistributedExecutor<R, T, S0s...> make_distributed_executor(
      R (*fn)(T &, S0s...), T, S1s &&...);

  DistributedExecutor();
  template <typename... S1s>
  Result run(RetT (*fn)(TR &, States...), TR task_range, S1s &&... states);
  template <typename... S1s>
  void start_async(RetT (*fn)(TR &, States...), TR task_range,
                   S1s &&... states);
  template <typename... S1s>
  void spawn_initial_workers(S1s &... states);
  template <typename... S1s>
  void add_workers(S1s &... states);
  void make_initial_dispatch(RetT (*fn)(TR &, States...), TR task_range);
  void check_workers();
  bool check_futures_and_redispatch();
  Result concat_results();
};

template <typename RetT, TaskRangeBased TR, typename... S0s, typename... S1s>
DistributedExecutor<RetT, TR, S0s...> make_distributed_executor(
    RetT (*fn)(TR &, S0s...), TR task_range, S1s &&... states);

}  // namespace nu

#include "nu/impl/dis_executor.ipp"
