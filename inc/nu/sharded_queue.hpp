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
  Queue(const Queue &);
  Queue &operator=(const Queue &);
  Queue(Queue &&) noexcept;
  Queue &operator=(Queue &&) noexcept;

  std::size_t size() const;
  bool empty() const;
  void emplace_back(Val v);
  void emplace_back_batch(std::vector<Val> v);
  Val front() const;
  Val back() const;
  void pop_front();
  std::optional<Val> try_dequeue();
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
  Mutex mutex_;
};

template <typename T, typename LL>
class ShardedQueue
    : public ShardedDataStructure<GeneralContainer<Queue<T>>, LL> {
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

 private:
  using Base = ShardedDataStructure<GeneralContainer<Queue<T>>, LL>;

  ShardedQueue();
  ShardedQueue(std::optional<typename Base::Hint> hint,
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
