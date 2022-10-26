#include <iostream>
#include <ranges>

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
inline void Queue<T>::emplace([[maybe_unused]] Key k, Val v) {
  queue_.push(std::move(v));
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
inline void Queue<T>::emplace_back(Val v) {
  queue_.push(v);
}

template <typename T>
inline void Queue<T>::emplace_back_batch(std::vector<Val> v) {
  BUG();
}

template <typename T>
inline void Queue<T>::pop_front() {
  queue_.pop();
}

template <typename T>
template <typename... S0s, typename... S1s>
inline void Queue<T>::for_all(void (*fn)(const Key &key, Val &val, S0s...),
                              S1s &&... states) {
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
    queue_.emplace_back(queue.queue_.top());
    queue.queue_.pop();
  }
}

template <typename T>
template <class Archive>
inline void Queue<T>::save(Archive &ar) const {
  ar(queue_);
}

template <typename T>
template <class Archive>
inline void Queue<T>::load(Archive &ar) {
  ar(queue_);
}

template <typename T, typename LL>
inline ShardedQueue<T, LL>::ShardedQueue() {}

template <typename T, typename LL>
inline ShardedQueue<T, LL>::ShardedQueue(
    std::optional<typename Base::Hint> hint)
    : Base(hint) {}

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
  Base::emplace_back(value);
}

template <typename T, typename LL>
inline void ShardedQueue<T, LL>::pop() {
  Base::pop_front();
}

template <typename T, typename LL>
inline ShardedQueue<T, LL> make_sharded_queue() {
  return ShardedQueue<T, LL>(std::nullopt);
}

}  // namespace nu
