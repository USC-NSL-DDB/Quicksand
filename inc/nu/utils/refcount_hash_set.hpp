#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sync.h>

#include "nu/commons.hpp"

namespace nu {

template <typename K, typename Allocator = std::allocator<K>>
class RefcountHashSet {
public:
  template <typename K1> void put(K1 &&k);
  template <typename K1> void remove(K1 &&k);
  // Can be only invoked once at a time.
  std::vector<K, Allocator> all_keys();

private:
  using Hash = std::hash<K>;
  using KeyEqual = std::equal_to<K>;
  using V = int;
  using RebindAlloc = std::allocator_traits<Allocator>::template rebind_alloc<
      std::pair<const K, V>>;

  std::unordered_map<K, V, Hash, KeyEqual, RebindAlloc> ref_counts_[kNumCores];
};
} // namespace nu

#include "nu/impl/refcount_hash_set.ipp"
