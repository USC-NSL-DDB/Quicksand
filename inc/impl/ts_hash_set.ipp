#include <functional>
#include <utility>

extern "C" {
#include <base/assert.h>
}

namespace nu {

template <typename K, typename Allocator, size_t NPartitions>
template <typename K1>
size_t ThreadSafeHashSet<K, Allocator, NPartitions>::partitioner(K1 &&k) {
  return std::hash<K>{}(std::forward<K1>(k)) % NPartitions;
}

template <typename K, typename Allocator, size_t NPartitions>
template <typename K1>
void ThreadSafeHashSet<K, Allocator, NPartitions>::put(K1 &&k) {
  auto idx = partitioner(std::forward<K1>(k));
  rt::ScopedLock<rt::Spin> lock(&spins_[idx]);
  sets_[idx].emplace(std::forward<K1>(k));
}

template <typename K, typename Allocator, size_t NPartitions>
template <typename K1>
bool ThreadSafeHashSet<K, Allocator, NPartitions>::remove(K1 &&k) {
  auto idx = partitioner(std::forward<K1>(k));
  rt::ScopedLock<rt::Spin> lock(&spins_[idx]);
  return sets_[idx].erase(std::forward<K1>(k));
}

template <typename K, typename Allocator, size_t NPartitions>
template <typename K1>
bool ThreadSafeHashSet<K, Allocator, NPartitions>::contains(K1 &&k) {
  auto idx = partitioner(std::forward<K1>(k));
  rt::ScopedLock<rt::Spin> lock(&spins_[idx]);
  return sets_[idx].contains(std::forward<K1>(k));
}

template <typename K, typename Allocator, size_t NPartitions>
std::vector<K, Allocator>
ThreadSafeHashSet<K, Allocator, NPartitions>::all_keys() {
  std::vector<K, Allocator> keys;
  for (size_t i = 0; i < NPartitions; i++) {
    rt::ScopedLock<rt::Spin> lock(&spins_[i]);
    for (const auto &k : sets_[i]) {
      keys.push_back(k);
    }
  }
  return keys;
}

template <typename K, typename Allocator, size_t NPartitions>
void ThreadSafeHashSet<K, Allocator, NPartitions>::for_each(
    const std::function<bool(const K &)> &fn) {
  for (size_t i = 0; i < NPartitions; i++) {
    rt::ScopedLock<rt::Spin> lock(&spins_[i]);
    for (const auto &k : sets_[i]) {
      if (!fn(k)) {
        return;
      }
    }
  }
}

} // namespace nu
