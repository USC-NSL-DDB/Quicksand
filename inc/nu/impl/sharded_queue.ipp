#include <iostream>
#include <ranges>

namespace nu {

template <typename T>
inline Queue<T>::Queue() : l_key_(0) {}

template <typename T>
inline Queue<T>::Queue(const Queue &o) : queue_(o.queue_), l_key_(o.l_key_) {}

template <typename T>
inline Queue<T> &Queue<T>::operator=(const Queue &o) {
  l_key_ = o.l_key_;
  queue_ = o.queue_;
  return *this;
}

template <typename T>
Queue<T>::Queue(Queue &&o) noexcept
    : queue_(std::move(o.queue_)), l_key_(std::move(o.l_key_)) {}

template <typename T>
Queue<T> &Queue<T>::operator=(Queue &&o) noexcept {
  l_key_ = std::move(o.l_key_);
  queue_ = std::move(o.queue_);
  return *this;
}

template <typename T>
inline std::size_t Queue<T>::size() const {
  ScopedLock<Mutex> guard(const_cast<Mutex *>(&mutex_));
  return queue_.size();
}

template <typename T>
inline bool Queue<T>::empty() const {
  ScopedLock<Mutex> guard(const_cast<Mutex *>(&mutex_));
  return queue_.empty();
}

template <typename T>
inline void Queue<T>::emplace([[maybe_unused]] Key k, Val v) {
  ScopedLock<Mutex> guard(const_cast<Mutex *>(&mutex_));
  queue_.push(std::move(v));
}

template <typename T>
inline Queue<T>::Val Queue<T>::front() const {
  ScopedLock<Mutex> guard(const_cast<Mutex *>(&mutex_));
  return queue_.front();
}

template <typename T>
inline Queue<T>::Val Queue<T>::back() const {
  ScopedLock<Mutex> guard(const_cast<Mutex *>(&mutex_));
  return queue_.back();
}

template <typename T>
inline void Queue<T>::emplace_back(Val v) {
  ScopedLock<Mutex> guard(const_cast<Mutex *>(&mutex_));
  queue_.push(v);
}

template <typename T>
inline void Queue<T>::emplace_back_batch(std::vector<Val> v) {
  BUG();
}

template <typename T>
inline void Queue<T>::pop_front() {
  ScopedLock<Mutex> guard(const_cast<Mutex *>(&mutex_));
  queue_.pop();
}

template <typename T>
inline std::optional<typename Queue<T>::Val> Queue<T>::try_dequeue() {
  ScopedLock<Mutex> guard(const_cast<Mutex *>(&mutex_));
  if (!queue_.size()) {
    return std::nullopt;
  }
  auto popped = queue_.front();
  queue_.pop();
  return popped;
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
  ScopedLock<Mutex> guard(const_cast<Mutex *>(&mutex_));
  while (!queue.queue_.empty()) {
    queue_.emplace_back(queue.queue_.top());
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
    std::optional<typename Base::Hint> hint,
    std::optional<std::size_t> size_bound)
    : Base(hint, size_bound) {}

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
  Base::enqueue(value);
}

template <typename T, typename LL>
inline void ShardedQueue<T, LL>::pop() {
  Base::pop_front();
}

template <typename T, typename LL>
inline T ShardedQueue<T, LL>::dequeue() {
  return Base::dequeue();
}

template <typename T, typename LL>
inline ShardedQueue<T, LL> make_sharded_queue() {
  auto hint = std::nullopt;
  auto size_bound = std::nullopt;
  return ShardedQueue<T, LL>(hint, size_bound);
}

template <typename T, typename LL>
inline ShardedQueue<T, LL> make_sharded_queue(std::size_t size_bound) {
  auto hint = std::nullopt;
  return ShardedQueue<T, LL>(hint, size_bound);
}

}  // namespace nu
