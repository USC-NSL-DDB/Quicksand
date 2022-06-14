#pragma once

#include <cstdint>
#include <utility>
#include <vector>

namespace nu {

template <typename K, typename V>
class ShardedPairCollection {
 public:
  using PairType = std::pair<K, V>;
  using ShardType = std::vector<PairType>;
  constexpr static uint32_t kDefaultShardSize = 2 << 20;

  ShardedPairCollection(uint32_t shard_size = kDefaultShardSize);
  template <typename K1, typename V1>
  void emplace_back(K1 &&k1, V1 &&v1);
  void emplace_back(PairType &&p);
  void emplace_back(const PairType &p);
  void emplace_back_batch(const ShardType &vec);
  void emplace_back_batch(ShardType &&vec);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(std::pair<const K, V> &, S0s...), S1s &&... states);
  ShardType collect();

 private:
  uint32_t shard_size_;
  Proclet<ShardType> shard_;
};

}  // namespace nu

#include "nu/impl/sharded_pair_collect.ipp"
