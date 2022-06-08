#include <functional>
#include <type_traits>
#include <utility>

extern "C" {
#include <base/assert.h>
}
#include <thread.h>

namespace nu {

template <typename K, typename V, typename Allocator>
void RCUHashMap<K, V, Allocator>::reader_lock() {
retry:
  rcu_.reader_lock();
  if (unlikely(rt::access_once(writer_barrier_))) {
    rcu_.reader_unlock();

    // Fast path: using rt::Yield() to wait for the writer.
    auto start_us = microtime();
    do {
      rt::Yield();
    } while (microtime() < start_us + kReaderWaitFastPathMaxUs &&
             unlikely(rt::access_once(writer_barrier_)));

    if (unlikely(rt::access_once(writer_barrier_))) {
      // Slow path: use Mutex + CondVar.
      mutex_.lock();
      while (unlikely(rt::access_once(writer_barrier_))) {
        cond_var_.wait(&mutex_);
      }
      mutex_.unlock();
    }

    goto retry;
  }
}

template <typename K, typename V, typename Allocator>
void RCUHashMap<K, V, Allocator>::reader_unlock() {
  rcu_.reader_unlock();
}

template <typename K, typename V, typename Allocator>
void RCUHashMap<K, V, Allocator>::writer_lock() {
  mutex_.lock();
  rt::access_once(writer_barrier_) = true;
  rcu_.writer_sync();
}

template <typename K, typename V, typename Allocator>
void RCUHashMap<K, V, Allocator>::writer_unlock() {
  rt::access_once(writer_barrier_) = false;
  cond_var_.signal_all();
  mutex_.unlock();
}

template <typename K, typename V, typename Allocator>
template <typename K1>
V *RCUHashMap<K, V, Allocator>::get(K1 &&k) {
  reader_lock();
  auto iter = map_.find(std::forward<K1>(k));
  V *ret;
  if (iter == map_.end()) {
    ret = nullptr;
  } else {
    ret = &iter->second;
  }
  reader_unlock();
  return ret;
}

template <typename K, typename V, typename Allocator>
template <typename K1, typename V1>
void RCUHashMap<K, V, Allocator>::put(K1 &&k, V1 &&v) {
  writer_lock();
  map_.emplace(std::forward<K1>(k), std::forward<V1>(v));
  writer_unlock();
}

template <typename K, typename V, typename Allocator>
template <typename K1, typename V1>
void RCUHashMap<K, V, Allocator>::put_if_not_exists(K1 &&k, V1 &&v) {
  writer_lock();
  map_.try_emplace(std::forward<K1>(k), std::forward<V1>(v));
  writer_unlock();
}

template <typename K, typename V, typename Allocator>
template <typename K1, typename... Args>
void RCUHashMap<K, V, Allocator>::emplace_if_not_exists(K1 &&k,
                                                        Args &&... args) {
  writer_lock();
  map_.try_emplace(std::forward<K1>(k), std::forward<Args>(args)...);
  writer_unlock();
}

template <typename K, typename V, typename Allocator>
template <typename K1, typename V1, typename V2>
bool RCUHashMap<K, V, Allocator>::update_if_equals(K1 &&k, V1 &&old_v,
                                                   V2 &&new_v) {
  bool updated = false;
  writer_lock();
  auto iter = map_.find(std::forward<K1>(k));
  if (iter != map_.end() && iter->second == std::forward<V1>(old_v)) {
    iter->second = std::forward<V2>(new_v);
    updated = true;
  }
  writer_unlock();
  return updated;
}

template <typename K, typename V, typename Allocator>
template <typename K1>
bool RCUHashMap<K, V, Allocator>::remove(K1 &&k) {
  writer_lock();
  auto ret = map_.erase(std::forward<K1>(k));
  writer_unlock();
  return ret;
}

template <typename K, typename V, typename Allocator>
template <typename K1, typename V1>
bool RCUHashMap<K, V, Allocator>::remove_if_equals(K1 &&k, V1 &&v) {
  bool removed = false;
  writer_lock();
  auto iter = map_.find(std::forward<K1>(k));
  if (iter != map_.end() && iter->second == std::forward<V1>(v)) {
    map_.erase(iter);
    removed = true;
  }
  writer_unlock();
  return removed;
}

template <typename K, typename V, typename Allocator>
void RCUHashMap<K, V, Allocator>::for_each(
    const std::function<bool(const std::pair<const K, V> &)> &fn) {
  reader_lock();
  for (const auto &p : map_) {
    if (!fn(p)) {
      reader_unlock();
      return;
    }
  }
  reader_unlock();
}

template <typename K, typename V, typename Allocator>
template <typename K1, typename RetT>
RetT RCUHashMap<K, V, Allocator>::apply(
    K1 &&k, const std::function<RetT(std::pair<const K, V> *)> &f) {
  reader_lock();
  auto iter = map_.find(std::forward<K1>(k));
  std::pair<const K, V> *pair_ptr = (iter == map_.end()) ? nullptr : &(*iter);
  if constexpr (std::is_same<RetT, void>::value) {
    f(pair_ptr);
    reader_unlock();
  } else {
    auto ret = f(pair_ptr);
    reader_unlock();
    return ret;
  }
}

}  // namespace nu
