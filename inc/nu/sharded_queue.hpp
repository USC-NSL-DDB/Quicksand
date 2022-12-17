#pragma once

#include <queue>

#include "sharded_ds.hpp"

namespace nu {

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
  std::optional<Val> pop_front();
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
  ShardedQueue(const ShardedQueue &) = default;
  ShardedQueue &operator=(const ShardedQueue &) = default;
  ShardedQueue(ShardedQueue &&) noexcept = default;
  ShardedQueue &operator=(ShardedQueue &&) noexcept = default;

  void push(const T &value);
  void push(T &&value);
  T front() const;
  T back() const;
  T pop();

 private:
  using Base = ShardedDataStructure<GeneralLockedContainer<Queue<T>>, LL>;

  ShardedQueue();
  ShardedQueue(std::optional<typename Base::ShardingHint> sharding_hint,
               std::optional<std::size_t> size_bound);
  friend class ProcletServer;
  template <typename T1, typename LL1>
  friend ShardedQueue<T1, LL1> make_sharded_queue();
  template <typename T1, typename LL1>
  friend ShardedQueue<T1, LL1> make_sharded_queue(std::size_t);
};

template <typename T, typename LL>
ShardedQueue<T, LL> make_sharded_queue();

template <typename T, typename LL>
ShardedQueue<T, LL> make_sharded_queue(std::size_t);

}  // namespace nu

#include "nu/impl/sharded_queue.ipp"
