#pragma once

#include <cstdint>
#include <functional>
#include <utility>

#include "nu/sealed_ds.hpp"
#include "nu/sharded_partitioner.hpp"

namespace nu {

template <typename K, typename V>
using ShardedSorted = SealedDS<ShardedPartitioner<K, V>>;

template <typename K, typename V>
class ShardedSorter {
 public:
  ShardedSorter(const ShardedSorter &) = delete;
  ShardedSorter &operator=(const ShardedSorter &) = delete;
  ShardedSorter(ShardedSorter &&);
  ShardedSorter &operator=(ShardedSorter &&);
  void emplace(K k, V v);
  void emplace(std::pair<K, V> p);
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

template <typename K, typename V>
ShardedSorter<K, V> make_sharded_sorter();

template <typename K, typename V>
ShardedSorter<K, V> make_sharded_sorter(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn);

}  // namespace nu

#include "nu/impl/sharded_sorter.ipp"
