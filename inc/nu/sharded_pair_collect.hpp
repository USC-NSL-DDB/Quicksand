#pragma once

#include <cstdint>
#include <map>
#include <utility>
#include <vector>

#include "nu/utils/scoped_lock.hpp"
#include "nu/utils/spinlock.hpp"

namespace nu {

template <typename K, typename V>
class ShardedPairCollection {
 public:
  using PairType = std::pair<K, V>;
  using ShardDataType = std::vector<PairType>;
  constexpr static uint32_t kDefaultShardSize = 2 << 20;

  ShardedPairCollection(uint32_t shard_size = kDefaultShardSize);
  template <typename K1, typename V1>
  void emplace_back(K1 &&k1, V1 &&v1);
  void emplace_back(PairType &&p);
  void emplace_back(const PairType &p);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(std::pair<const K, V> &, S0s...), S1s &&... states);
  ShardDataType collect();

 private:

  class ShardingMapping;
  class Shard {
   public:
    Shard(WeakProclet<ShardingMapping> mapping, uint32_t shard_size);
    ShardDataType get_data();
    std::pair<ScopedLock<SpinLock>, ShardDataType *> get_data_ptr();
    void emplace_back(PairType &&p);
    void set_data(ShardDataType v);

   private:
    SpinLock spin_;
    ShardDataType data_;
    uint32_t shard_size_;
    WeakProclet<ShardingMapping> mapping_;
  };

  class ShardingMapping {
   public:
    ShardingMapping();
    template <typename K1>
    WeakProclet<Shard> get_shard(K1 k1);
    std::vector<WeakProclet<Shard>> get_all_shards();
    template <typename K1>
    void update_mapping(K1 k1, Proclet<Shard> shard);
    void set_initial_shard(Proclet<Shard> shard);

   private:
    std::map<K, int, std::greater<K>> mapping_;
    std::vector<Proclet<Shard>> shards_;
  };

  Proclet<ShardingMapping> mapping_;
  uint32_t shard_size_;
};

}  // namespace nu

#include "nu/impl/sharded_pair_collect.ipp"
