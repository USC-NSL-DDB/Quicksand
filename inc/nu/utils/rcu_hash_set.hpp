#pragma once

#include <functional>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sync.h>

#include "nu/utils/rcu_lock.hpp"

namespace nu {

template <typename K, typename Allocator = std::allocator<K>>
class RCUHashSet {
public:
  template <typename K1> void put(K1 &&k);
  template <typename K1> bool remove(K1 &&k);
  template <typename K1> bool contains(K1 &&k);
  void for_each(const std::function<bool(const K &)> &fn);

private:
  using Hash = std::hash<K>;
  using KeyEqual = std::equal_to<K>;

  std::unordered_set<K, Hash, KeyEqual, Allocator> set_;
  rt::Mutex mutex_;
  RCULock rcu_;
  bool writer_barrier_ = false;
};
} // namespace nun

#include "nu/impl/rcu_hash_set.ipp"
