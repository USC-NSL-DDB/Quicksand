#pragma once

#include <optional>
#include <queue>

#include "commons.hpp"
#include "queue_range.hpp"
#include "sharded_ds.hpp"

namespace nu {

template <typename RetT, TaskRangeBased TR, typename... States>
class DistributedExecutor;

template <typename T, typename LL>
class ShardedQueue;

template <class Q>
concept ShardedQueueBased = requires {
  requires is_base_of_template_v<Q, ShardedQueue>;
};

template <typename RetT, QueueRangeBased QR, typename... S0s, typename... S1s>
DistributedExecutor<RetT, TaskRange<QR>, S0s...> make_distributed_executor(
    RetT (*fn)(TaskRange<QR> &, S0s...), TaskRange<QR> queue_range,
    S1s &&... states);

template <typename T>
class Queue {
 public:
  using Key = std::size_t;
  using Val = T;

  Queue();
  Queue(const Queue &) = default;
  Queue &operator=(const Queue &) = default;
  Queue(Queue &&) noexcept = default;
  Queue &operator=(Queue &&) noexcept = default;

  std::size_t size() const;
  bool empty() const;
  std::size_t push_back(Val v);
  std::size_t push_back_batch(std::vector<Val> vec);
  Val front() const;
  Val back() const;
  std::vector<Val> try_pop_front(std::size_t num);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  void split(Key *mid_k, Queue *latter_half);
  void merge(Queue queue);
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  std::queue<T> queue_;
  Key l_key_;
};

template <typename T, typename LL>
class ShardedQueue
    : public ShardedDataStructure<GeneralLockedContainer<Queue<T>>, LL> {
 public:
  ShardedQueue();
  ShardedQueue(const ShardedQueue &) = default;
  ShardedQueue &operator=(const ShardedQueue &) = default;
  ShardedQueue(ShardedQueue &&) noexcept = default;
  ShardedQueue &operator=(ShardedQueue &&) noexcept = default;

  void push(const T &value);
  void push(T &&value);
  T front() const;
  T back() const;
  T pop();
  std::vector<T> try_pop(std::size_t num);

 private:
  using Base = ShardedDataStructure<GeneralLockedContainer<Queue<T>>, LL>;

  ShardedQueue(std::optional<typename Base::ShardingHint> sharding_hint,
               std::optional<std::size_t> size_bound,
               std::optional<NodeIP> pinned_ip);
  friend class ProcletServer;
  template <typename T1, BoolIntegral LL1, BoolIntegral Insertable>
  friend class QueueTaskRangeImpl;
  template <typename T1, typename LL1>
  friend ShardedQueue<T1, LL1> make_sharded_queue(std::optional<std::size_t>,
                                                  std::optional<NodeIP>);
  template <class... Types>
  friend class tuple;
};

template <typename T, typename LL>
ShardedQueue<T, LL> make_sharded_queue(
    std::optional<std::size_t> size_bound = std::nullopt,
    std::optional<NodeIP> pinned_ip = std::nullopt);

}  // namespace nu

#include "nu/impl/sharded_queue.ipp"
