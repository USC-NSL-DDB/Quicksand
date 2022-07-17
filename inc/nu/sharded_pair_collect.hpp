#pragma once

#include <vector>

#include "nu/sharded_ds.hpp"

namespace nu {

template <typename K, typename V>
class PairCollection : public GeneralContainer<K, V> {
 public:
  std::size_t size() const override;
  bool empty() const override;
  void clear() override;
  void reserve(std::size_t size) override;
  void emplace(K k, V v) override;
  void emplace_batch(GeneralContainer<K, V> &container) override;
  void split(K *mid_k_ptr, GeneralContainer<K, V> *latter_half_ptr) override;
  void merge(GeneralContainer<K, V> &container) override;
  void for_all(std::function<void(std::pair<const K, V> &)> fn) override;
  void save(cereal::BinaryOutputArchive &ar) const override;
  void load(cereal::BinaryInputArchive &ar) override;

  std::vector<std::pair<K, V>> &unwrap();

 private:
  std::vector<std::pair<K, V>> data_;
};

template <typename K, typename V>
class ShardedPairCollection
    : public ShardedDataStructure<PairCollection<K, V>> {
 public:
  constexpr static uint32_t kDefaultMaxShardBytes = 16 << 20;
  constexpr static uint32_t kDefaultMaxCacheBytes = 100 << 10;

  ShardedPairCollection() = default;
  ShardedPairCollection(const ShardedPairCollection &) = default;
  ShardedPairCollection &operator=(const ShardedPairCollection &) = default;
  ShardedPairCollection(ShardedPairCollection &&) noexcept = default;
  ShardedPairCollection &operator=(ShardedPairCollection &&) noexcept = default;

 private:
  using Base = ShardedDataStructure<PairCollection<K, V>>;
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
