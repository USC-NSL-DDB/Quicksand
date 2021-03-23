#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <utility>

#include "sync.h"

namespace nu {

template <typename K, typename V,
          typename Allocator = std::allocator<std::pair<const K, V>>,
          size_t NPartitions = 64>
class ThreadSafeHashMap {
public:
  template <typename K1> V &get(K1 &&k);
  template <typename K1, typename V1> void put(K1 &&k, V1 &&v);
  template <typename K1> bool remove(K1 &&k);
  template <typename K1> bool contains(K1 &&k);
  template <typename K1> V get_and_remove(K1 &&k);

private:
  using Hash = std::hash<K>;
  using KeyEqual = std::equal_to<K>;
  std::unordered_map<K, V, Hash, KeyEqual, Allocator> maps_[NPartitions];
  rt::Spin spins_[NPartitions];

  template <typename K1> size_t partitioner(K1 &&k);
};
} // namespace nu

#include "impl/ts_hash_map.ipp"
