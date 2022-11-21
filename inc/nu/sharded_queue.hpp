#pragma once

#include <queue>

#include "sharded_ds.hpp"

namespace nu {

// FIFO Queue
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
  void emplace(Key k, Val v);
  void emplace_back(Val v);
  void emplace_back_batch(std::vector<Val> v);
  Val front() const;
  Val back() const;
  void pop_front();
  Val dequeue();
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

  T front() const;
  T back() const;
  void push(const T &value);
  void pop();
  T dequeue();
  // TODO: swap
  // void swap();

 private:
  using Base = ShardedDataStructure<GeneralLockedContainer<Queue<T>>, LL>;

  ShardedQueue();
  ShardedQueue(std::optional<typename Base::Hint> hint);
  friend class ProcletServer;
  template <typename T1, typename LL1>
  friend ShardedQueue<T1, LL1> make_sharded_queue();
};

template <typename T, typename LL>
ShardedQueue<T, LL> make_sharded_queue();

}  // namespace nu

#include "nu/impl/sharded_queue.ipp"
