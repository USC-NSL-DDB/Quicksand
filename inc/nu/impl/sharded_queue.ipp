#include <iostream>
#include <ranges>

#include "nu/dis_executor.hpp"

namespace nu {

template <typename T>
inline Queue<T>::Queue() : l_key_(0) {}

template <typename T>
inline std::size_t Queue<T>::size() const {
  return queue_.size();
}

template <typename T>
inline bool Queue<T>::empty() const {
  return queue_.empty();
}

template <typename T>
inline Queue<T>::Val Queue<T>::front() const {
  return queue_.front();
}

template <typename T>
inline Queue<T>::Val Queue<T>::back() const {
  return queue_.back();
}

template <typename T>
inline std::size_t Queue<T>::push_back(Val v) {
  queue_.push(std::move(v));
  return queue_.size();
}

template <typename T>
inline std::size_t Queue<T>::push_back_batch(std::vector<Val> vec) {
  for (auto &v : vec) {
    queue_.push(std::move(v));
  }

  return queue_.size();
}

template <typename T>
inline std::vector<T> Queue<T>::try_pop_front(std::size_t num) {
  std::vector<T> elems;

  num = std::min(num, queue_.size());
  elems.reserve(num);
  while (num--) {
    elems.emplace_back(std::move(queue_.front()));
    queue_.pop();
  }
  return elems;
}

template <typename T>
template <typename... S0s, typename... S1s>
inline void Queue<T>::for_all(void (*fn)(const Key &key, Val &val, S0s...),
                              S1s &&...states) {
  BUG();
}

template <typename T>
inline void Queue<T>::split(Key *mid_k, Queue *latter_half) {
  latter_half->l_key_ = l_key_ + queue_.size();
  *mid_k = latter_half->l_key_;
}

template <typename T>
inline void Queue<T>::merge(Queue queue) {
  while (!queue.queue_.empty()) {
    queue_.push(queue.queue_.front());
    queue.queue_.pop();
  }
}

template <typename T>
template <class Archive>
inline void Queue<T>::save(Archive &ar) const {
  ar(queue_, l_key_);
}

template <typename T>
template <class Archive>
inline void Queue<T>::load(Archive &ar) {
  ar(queue_, l_key_);
}

template <typename T, typename LL>
inline ShardedQueue<T, LL>::ShardedQueue() {}

template <typename T, typename LL>
inline ShardedQueue<T, LL>::ShardedQueue(
    std::optional<typename Base::ShardingHint> sharding_hint,
    std::optional<NodeIP> pinned_ip)
    : Base(sharding_hint, pinned_ip) {}

template <typename T, typename LL>
inline T ShardedQueue<T, LL>::front() const {
  return Base::front();
}

template <typename T, typename LL>
inline T ShardedQueue<T, LL>::back() const {
  return Base::back();
}

template <typename T, typename LL>
void ShardedQueue<T, LL>::push(const T &value) {
  Base::push_back(value);
}

template <typename T, typename LL>
void ShardedQueue<T, LL>::push(T &&value) {
  Base::push_back(std::move(value));
}

template <typename T, typename LL>
inline T ShardedQueue<T, LL>::pop() {
  return Base::pop_front();
}

template <typename T, typename LL>
inline std::vector<T> ShardedQueue<T, LL>::try_pop(std::size_t num) {
  return Base::try_pop_front(num);
}

template <typename T, typename LL>
inline ShardedQueue<T, LL> make_sharded_queue(std::optional<NodeIP> pinned_ip) {
  auto hint = std::nullopt;
  return ShardedQueue<T, LL>(hint, pinned_ip);
}

}  // namespace nu
