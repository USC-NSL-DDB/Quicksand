#include <functional>
#include <utility>

extern "C" {
#include <base/assert.h>
}
#include <thread.h>

namespace nu {

template <typename K, typename Allocator>
void RCUHashSet<K, Allocator>::reader_lock() {
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

template <typename K, typename Allocator>
void RCUHashSet<K, Allocator>::reader_unlock() {
  rcu_.reader_unlock();
}

template <typename K, typename Allocator>
void RCUHashSet<K, Allocator>::writer_lock() {
  mutex_.lock();
  rt::access_once(writer_barrier_) = true;
  rcu_.writer_sync();
}

template <typename K, typename Allocator>
void RCUHashSet<K, Allocator>::writer_unlock() {
  rt::access_once(writer_barrier_) = false;
  cond_var_.signal_all();
  mutex_.unlock();
}

template <typename K, typename Allocator>
template <typename K1>
void RCUHashSet<K, Allocator>::put(K1 &&k) {
  writer_lock();
  set_.emplace(std::forward<K1>(k));
  writer_unlock();
}

template <typename K, typename Allocator>
template <typename K1>
bool RCUHashSet<K, Allocator>::remove(K1 &&k) {
  writer_lock();
  auto ret = set_.erase(std::forward<K1>(k));
  writer_unlock();
  return ret;
}

template <typename K, typename Allocator>
template <typename K1>
bool RCUHashSet<K, Allocator>::contains(K1 &&k) {
  reader_lock();
  auto ret = set_.contains(std::forward<K1>(k));
  reader_unlock();
  return ret;
}

template <typename K, typename Allocator>
void RCUHashSet<K, Allocator>::for_each(
    const std::function<bool(const K &)> &fn) {
  reader_lock();
  for (const auto &k : set_) {
    if (!fn(k)) {
      reader_unlock();
      return;
    }
  }
  reader_unlock();
}

}  // namespace nu
