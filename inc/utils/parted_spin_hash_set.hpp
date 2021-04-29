#pragma once

#include <functional>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sync.h>

namespace nu {

template <typename K, typename Allocator = std::allocator<K>,
          size_t NPartitions = 29>
class PartitionedSpinHashSet {
public:
  template <typename K1> void put(K1 &&k);
  template <typename K1> bool remove(K1 &&k);
  template <typename K1> bool contains(K1 &&k);
  std::vector<K, Allocator> all_keys();
  void for_each(const std::function<bool(const K &)> &fn);

private:
  using Hash = std::hash<K>;
  using KeyEqual = std::equal_to<K>;

  struct alignas(kCacheLineBytes) AlignedSpin {
    rt::Spin spin;
  };

  std::unordered_set<K, Hash, KeyEqual, Allocator> sets_[NPartitions];
  AlignedSpin spins_[NPartitions];

  template <typename K1> size_t partitioner(K1 &&k);
};
} // namespace nu

#include "impl/parted_spin_hash_set.ipp"
