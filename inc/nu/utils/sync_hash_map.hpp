#pragma once

#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <sync.h>

namespace nu {

template <size_t NBuckets, typename K, typename V, typename Hash = std::hash<K>,
          typename KeyEqual = std::equal_to<K>,
          typename Allocator = std::allocator<std::pair<const K, V>>,
          typename Lock = rt::Spin>
class SyncHashMap {
public:
  SyncHashMap();
  template <typename K1> std::optional<V> get(K1 &&k);
  template <typename K1>
  std::optional<V> get_with_hash(K1 &&k, uint64_t key_hash);
  template <typename K1, typename V1> void put(K1 &&k, V1 &&v);
  template <typename K1, typename V1>
  void put_with_hash(K1 &&k, V1 &&v, uint64_t key_hash);
  template <typename K1> bool remove(K1 &&k);
  template <typename K1> bool remove_with_hash(K1 &&k, uint64_t key_hash);
  template <typename K1, typename RetT, typename... A0s, typename... A1s>
  RetT apply(K1 &&k, RetT (*fn)(std::pair<const K, V> &, A0s...),
             A1s &&... args);
  template <typename K1, typename RetT, typename... A0s, typename... A1s>
  RetT apply_with_hash(K1 &&k, uint64_t key_hash,
                       RetT (*fn)(std::pair<const K, V> &, A0s...),
                       A1s &&... args);
  std::vector<std::pair<K, V>> get_all_pairs();

private:
  struct BucketNode {
    uint64_t key_hash;
    void *pair;
    BucketNode *next;
  };

  using Pair = std::pair<const K, V>;
  using BucketNodeAllocator =
      std::allocator_traits<Allocator>::template rebind_alloc<BucketNode>;

  BucketNode buckets_[NBuckets];
  Lock locks_[NBuckets];
};
} // namespace nu

#include "nu/impl/sync_hash_map.ipp"
