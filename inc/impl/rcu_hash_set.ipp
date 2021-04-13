#include <functional>
#include <utility>

extern "C" {
#include <base/assert.h>
}

namespace nu {

template <typename K, typename Allocator, size_t NPartitions>
template <typename K1>
size_t RCUHashSet<K, Allocator, NPartitions>::partitioner(K1 &&k) {
  return std::hash<K>{}(std::forward<K1>(k)) % NPartitions;
}

template <typename K, typename Allocator, size_t NPartitions>
template <typename K1>
void RCUHashSet<K, Allocator, NPartitions>::put(K1 &&k) {
  auto idx = partitioner(std::forward<K1>(k));
  rcus_[idx].rcu.writer_sync_fn([&]() {
    rt::ScopedLock<rt::Spin> lock(&spins_[idx].spin);
    sets_[idx].emplace(std::forward<K1>(k));
  });
}

template <typename K, typename Allocator, size_t NPartitions>
template <typename K1>
bool RCUHashSet<K, Allocator, NPartitions>::remove(K1 &&k) {
  auto idx = partitioner(std::forward<K1>(k));
  bool ret;
  rcus_[idx].rcu.writer_sync_fn([&]() {
    rt::ScopedLock<rt::Spin> lock(&spins_[idx].spin);
    ret = sets_[idx].erase(std::forward<K1>(k));
  });
  return ret;
}

template <typename K, typename Allocator, size_t NPartitions>
template <typename K1>
bool RCUHashSet<K, Allocator, NPartitions>::contains(K1 &&k) {
  auto idx = partitioner(std::forward<K1>(k));
  rcus_[idx].rcu.reader_lock();
  auto ret = sets_[idx].contains(std::forward<K1>(k));
  rcus_[idx].rcu.reader_unlock();
  return ret;
}

template <typename K, typename Allocator, size_t NPartitions>
void RCUHashSet<K, Allocator, NPartitions>::for_each(
    const std::function<bool(const K &)> &fn) {
  for (size_t i = 0; i < NPartitions; i++) {
    rcus_[i].rcu.reader_lock();
    for (const auto &k : sets_[i]) {
      if (!fn(k)) {
        rcus_[i].rcu.reader_unlock();
        return;
      }
    }
    rcus_[i].rcu.reader_unlock();
  }
}

} // namespace nu
