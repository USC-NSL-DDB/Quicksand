#include <functional>
#include <type_traits>
#include <utility>

extern "C" {
#include <base/assert.h>
}
#include <thread.h>

namespace nu {

template <typename K, typename V, typename Allocator>
template <typename K1>
V *RCUHashMap<K, V, Allocator>::get(K1 &&k) {
retry:
  rcu_.reader_lock();
  if (unlikely(ACCESS_ONCE(writer_barrier_))) {
    rcu_.reader_unlock();
    while (unlikely(ACCESS_ONCE(writer_barrier_))) {
      rt::Yield();
    }
    goto retry;
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
template <typename K1, typename... Args>
void RCUHashMap<K, V, Allocator>::emplace_if_not_exists(K1 &&k,
                                                        Args &&... args) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);
  ACCESS_ONCE(writer_barrier_) = true;
  rcu_.writer_sync();
  map_.try_emplace(std::forward<K1>(k), std::forward<Args>(args)...);
  ACCESS_ONCE(writer_barrier_) = false;
}

template <typename K, typename V, typename Allocator>
template <typename K1, typename V1, typename V2>
bool RCUHashMap<K, V, Allocator>::update_if_equals(K1 &&k, V1 &&old_v,
                                                   V2 &&new_v) {
  bool updated = false;
  rt::ScopedLock<rt::Mutex> lock(&mutex_);
  ACCESS_ONCE(writer_barrier_) = true;
  rcu_.writer_sync();
  auto iter = map_.find(std::forward<K1>(k));
  if (iter != map_.end() && iter->second == std::forward<V1>(old_v)) {
    iter->second = std::forward<V2>(new_v);
    updated = true;
  }
  ACCESS_ONCE(writer_barrier_) = false;
  return updated;
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

template <typename K, typename V, typename Allocator>
template <typename K1, typename V1>
bool RCUHashMap<K, V, Allocator>::remove_if_equals(K1 &&k, V1 &&v) {
  bool removed = false;
  rt::ScopedLock<rt::Mutex> lock(&mutex_);
  ACCESS_ONCE(writer_barrier_) = true;
  rcu_.writer_sync();
  auto iter = map_.find(std::forward<K1>(k));
  if (iter != map_.end() && iter->second == std::forward<V1>(v)) {
    map_.erase(iter);
    removed = true;
  }
  ACCESS_ONCE(writer_barrier_) = false;
  return removed;
}

template <typename K, typename V, typename Allocator>
void RCUHashMap<K, V, Allocator>::for_each(
    const std::function<bool(const std::pair<const K, V> &)> &fn) {
retry:
  rcu_.reader_lock();
  if (unlikely(ACCESS_ONCE(writer_barrier_))) {
    rcu_.reader_unlock();
    while (unlikely(ACCESS_ONCE(writer_barrier_))) {
      rt::Yield();
    }
    goto retry;
  }
  for (const auto &p : map_) {
    if (!fn(p)) {
      rcu_.reader_unlock();
      return;
    }
  }
  rcu_.reader_unlock();
}

template <typename K, typename V, typename Allocator>
template <typename K1, typename RetT>
RetT RCUHashMap<K, V, Allocator>::apply(
    K1 &&k, const std::function<RetT(std::pair<const K, V> *)> &f) {
retry:
  rcu_.reader_lock();
  if (unlikely(ACCESS_ONCE(writer_barrier_))) {
    rcu_.reader_unlock();
    while (unlikely(ACCESS_ONCE(writer_barrier_))) {
      rt::Yield();
    }
    goto retry;
  }
  auto iter = map_.find(std::forward<K1>(k));
  std::pair<const K, V> *pair_ptr = (iter == map_.end()) ? nullptr : &(*iter);
  if constexpr (std::is_same<RetT, void>::value) {
    f(pair_ptr);
    rcu_.reader_unlock();
  } else {
    auto ret = f(pair_ptr);
    rcu_.reader_unlock();
    return ret;
  }
}

} // namespace nu

