#pragma once

#include <vector>

#include "nu/sharded_ds.hpp"

namespace nu {

template <typename K, typename V>
class PairCollection {
 public:
  using Key = K;
  using Val = V;

  PairCollection();
  PairCollection(std::size_t size);
  PairCollection(const PairCollection &);
  PairCollection &operator=(const PairCollection &);
  PairCollection(PairCollection &&) noexcept;
  PairCollection &operator=(PairCollection &&) noexcept;
  std::size_t size() const;
  bool empty() const;
  void clear();
  void emplace(K k, V v);
  void emplace_batch(PairCollection &&pc);
  void split(K *mid_k_ptr, PairCollection *latter_half_ptr);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(std::pair<const Key, Val> &, S0s...),
               S1s &&... states);
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

  std::vector<std::pair<K, V>> &get_data();

 private:
  std::vector<std::pair<K, V>> data_;
};

template <typename K, typename V>
using PairCollectionContainer = GeneralContainer<PairCollection<K, V>>;

template <typename K, typename V>
class ShardedPairCollection
    : public ShardedDataStructure<PairCollectionContainer<K, V>> {
 public:
  constexpr static uint32_t kDefaultMaxShardBytes = 16 << 20;
  constexpr static uint32_t kDefaultMaxCacheBytes = 100 << 10;

  ShardedPairCollection() = default;
  ShardedPairCollection(const ShardedPairCollection &) = default;
  ShardedPairCollection &operator=(const ShardedPairCollection &) = default;
  ShardedPairCollection(ShardedPairCollection &&) noexcept = default;
  ShardedPairCollection &operator=(ShardedPairCollection &&) noexcept = default;

 private:
  using Base = ShardedDataStructure<PairCollectionContainer<K, V>>;
  ShardedPairCollection(std::optional<K> initial_l_key,
                        std::optional<K> initial_r_key,
                        uint32_t max_shard_bytes, uint32_t max_cache_bytes);
  ShardedPairCollection(uint64_t num, K estimated_min_key,
                        std::function<void(K &, uint64_t)> key_inc_fn,
                        uint32_t max_shard_bytes, uint32_t max_cache_bytes);
  template <typename K1, typename V1>
  friend ShardedPairCollection<K1, V1> make_sharded_pair_collection(
      uint32_t max_shard_bytes, uint32_t max_cache_bytes);
  template <typename K1, typename V1>
  friend ShardedPairCollection<K1, V1> make_sharded_pair_collection(
      uint64_t num, K1 estimated_min_key,
      std::function<void(K1 &, uint64_t)> key_inc_fn, uint32_t max_shard_bytes,
      uint32_t max_cache_bytes);
};

template <typename K, typename V>
ShardedPairCollection<K, V> make_sharded_pair_collection(
    uint32_t max_shard_bytes =
        ShardedPairCollection<K, V>::kDefaultMaxShardBytes,
    uint32_t max_cache_bytes =
        ShardedPairCollection<K, V>::kDefaultMaxCacheBytes);

template <typename K, typename V>
ShardedPairCollection<K, V> make_sharded_pair_collection(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn,
    uint32_t max_shard_bytes =
        ShardedPairCollection<K, V>::kDefaultMaxShardBytes,
    uint32_t max_cache_bytes =
        ShardedPairCollection<K, V>::kDefaultMaxCacheBytes);

}  // namespace nu

#include "nu/impl/sharded_pair_collect.ipp"
