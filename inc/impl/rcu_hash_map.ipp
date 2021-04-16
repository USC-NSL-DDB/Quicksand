#include <functional>
#include <utility>

extern "C" {
#include <base/assert.h>
}

namespace nu {

template <typename K, typename V, typename Allocator>
template <typename K1>
V *RCUHashMap<K, V, Allocator>::get(K1 &&k) {
  rcu_.reader_lock();
  while (unlikely(ACCESS_ONCE(writer_barrier_))) {
    rcu_.reader_unlock();
    thread_yield();
    rcu_.reader_lock();
  }
  auto iter = map_.find(std::forward<K1>(k));
  V *ret;
  if (iter == map_.end()) {
    ret = nullptr;
  } else {
    ret = &iter->second;
  }
  rcu_.reader_unlock();
  return ret;
}

template <typename K, typename V, typename Allocator>
template <typename K1, typename V1>
void RCUHashMap<K, V, Allocator>::put(K1 &&k, V1 &&v) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);  
  ACCESS_ONCE(writer_barrier_) = true;
  rcu_.writer_sync();
  map_.emplace(std::forward<K1>(k), std::forward<V1>(v));
  ACCESS_ONCE(writer_barrier_) = false;
}

template <typename K, typename V, typename Allocator>
template <typename K1, typename V1>
void RCUHashMap<K, V, Allocator>::put_if_not_exists(K1 &&k, V1 &&v) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);
  ACCESS_ONCE(writer_barrier_) = true;
  rcu_.writer_sync();
  map_.try_emplace(std::forward<K1>(k), std::forward<V1>(v));
  ACCESS_ONCE(writer_barrier_) = false;
}

template <typename K, typename V, typename Allocator>
template <typename K1>
bool RCUHashMap<K, V, Allocator>::remove(K1 &&k) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);
  ACCESS_ONCE(writer_barrier_) = true;
  rcu_.writer_sync();
  auto ret = map_.erase(std::forward<K1>(k));
  ACCESS_ONCE(writer_barrier_) = false;
  return ret;
}

} // namespace nu
