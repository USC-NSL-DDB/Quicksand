#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>

#include <sync.h>

namespace nu {

template <typename K, typename V,
          typename Allocator = std::allocator<std::pair<const K, V>>>
class RCUHashMap {
public:
  template <typename K1> V *get(K1 &&k);
  template <typename K1, typename V1> void put(K1 &&k, V1 &&v);
  template <typename K1, typename V1> void put_if_not_exists(K1 &&k, V1 &&v);
  template <typename K1> bool remove(K1 &&k);

private:
  using Hash = std::hash<K>;
  using KeyEqual = std::equal_to<K>;

  std::unordered_map<K, V, Hash, KeyEqual, Allocator> map_;
  rt::Mutex mutex_;
  RCULock rcu_;
  bool writer_barrier_ = false;
};
} // namespace nu

#include "impl/rcu_hash_map.ipp"
