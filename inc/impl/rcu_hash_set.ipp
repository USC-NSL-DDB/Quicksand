#include <functional>
#include <utility>

extern "C" {
#include <base/assert.h>
}
#include <thread.h>

namespace nu {

template <typename K, typename Allocator>
template <typename K1>
void RCUHashSet<K, Allocator>::put(K1 &&k) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);
  ACCESS_ONCE(writer_barrier_) = true;
  rcu_.writer_sync();
  set_.emplace(std::forward<K1>(k));
  ACCESS_ONCE(writer_barrier_) = false;
}

template <typename K, typename Allocator>
template <typename K1>
bool RCUHashSet<K, Allocator>::remove(K1 &&k) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);
  ACCESS_ONCE(writer_barrier_) = true;
  rcu_.writer_sync();
  auto ret = set_.erase(std::forward<K1>(k));
  ACCESS_ONCE(writer_barrier_) = false;
  return ret;
}

template <typename K, typename Allocator>
template <typename K1>
bool RCUHashSet<K, Allocator>::contains(K1 &&k) {
retry:
  rcu_.reader_lock();
  if (unlikely(ACCESS_ONCE(writer_barrier_))) {
    rcu_.reader_unlock();
    while (unlikely(ACCESS_ONCE(writer_barrier_))) {
      rt::Yield();
    }
    goto retry;
  }
  auto ret = set_.contains(std::forward<K1>(k));
  rcu_.reader_unlock();
  return ret;
}

template <typename K, typename Allocator>
void RCUHashSet<K, Allocator>::for_each(
    const std::function<bool(const K &)> &fn) {
retry:
  rcu_.reader_lock();
  if (unlikely(ACCESS_ONCE(writer_barrier_))) {
    rcu_.reader_unlock();
    while (unlikely(ACCESS_ONCE(writer_barrier_))) {
      rt::Yield();
    }
    goto retry;
  }
  for (const auto &k : set_) {
    if (!fn(k)) {
      rcu_.reader_unlock();
      return;
    }
  }
  rcu_.reader_unlock();
}

} // namespace nu
