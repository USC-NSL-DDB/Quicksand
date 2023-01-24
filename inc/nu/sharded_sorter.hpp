#pragma once

#include <cstdint>
#include <functional>
#include <type_traits>
#include <utility>

#include "nu/sealed_ds.hpp"
#include "nu/sharded_partitioner.hpp"

namespace nu {

template <typename K, typename V = ErasedType>
using ShardedSorted = SealedDS<ShardedPartitioner<K, V>>;

template <typename K, typename V = ErasedType>
class ShardedSorter {
 public:
  ShardedSorter(const ShardedSorter &) = default;
  ShardedSorter &operator=(const ShardedSorter &) = default;
  ShardedSorter(ShardedSorter &&) = default;
  ShardedSorter &operator=(ShardedSorter &&) = default;
  void insert(K k) requires std::is_same_v<V, ErasedType>;
  void insert(K k, V v) requires(!std::is_same_v<V, ErasedType>);
  void insert(std::pair<K, V> p) requires(!std::is_same_v<V, ErasedType>);
  ShardedSorted<K, V> sort();

 private:
  ShardedPartitioner<K, V> sharded_pn_;

  template <typename K1, typename V1>
  friend ShardedSorter<K1, V1> make_sharded_sorter();
  template <typename K1, typename V1>
  friend ShardedSorter<K1, V1> make_sharded_sorter(
      uint64_t num, K1 estimated_min_key,
      std::function<void(K1 &, uint64_t)> key_inc_fn);

  ShardedSorter(ShardedPartitioner<K, V> &&sharded_pn);
};

template <typename K, typename V = ErasedType>
ShardedSorter<K, V> make_sharded_sorter();

template <typename K, typename V = ErasedType>
ShardedSorter<K, V> make_sharded_sorter(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn);

}  // namespace nu

#include "nu/impl/sharded_sorter.ipp"
