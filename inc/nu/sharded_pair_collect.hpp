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
  PairCollection(std::size_t capacity);
  PairCollection(const PairCollection &);
  PairCollection &operator=(const PairCollection &);
  PairCollection(PairCollection &&) noexcept;
  PairCollection &operator=(PairCollection &&) noexcept;
  ~PairCollection();
  std::size_t size() const;
  std::size_t capacity() const;
  bool empty() const;
  void clear();
  void emplace(K k, V v);
  void emplace_batch(PairCollection &&pc);
  std::pair<Key, PairCollection> split();
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(std::pair<const Key, Val> &, S0s...),
               S1s &&... states);
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

  std::pair<K, V> *data();

 private:
  std::pair<K, V> *data_;
  std::size_t size_;
  std::size_t capacity_;
  bool ownership_;

  void expand();
};

template <typename K, typename V>
using PairCollectionContainer = GeneralContainer<PairCollection<K, V>>;

template <typename K, typename V>
class ShardedPairCollection
    : public ShardedDataStructure<PairCollectionContainer<K, V>> {
 public:
  constexpr static uint32_t kDefaultMaxShardBytes = 16 << 20;
  constexpr static uint32_t kDefaultMaxBatchBytes = 100 << 10;

  ShardedPairCollection() = default;
  ShardedPairCollection(const ShardedPairCollection &) = default;
  ShardedPairCollection &operator=(const ShardedPairCollection &) = default;
  ShardedPairCollection(ShardedPairCollection &&) noexcept = default;
  ShardedPairCollection &operator=(ShardedPairCollection &&) noexcept = default;

 private:
  using Base = ShardedDataStructure<PairCollectionContainer<K, V>>;
  ShardedPairCollection(std::optional<K> initial_l_key,
                        std::optional<K> initial_r_key,
                        uint32_t max_shard_bytes, uint32_t max_batch_bytes);
  ShardedPairCollection(uint64_t num, K estimated_min_key,
                        std::function<void(K &, uint64_t)> key_inc_fn,
                        uint32_t max_shard_bytes, uint32_t max_batch_bytes);
  template <typename K1, typename V1>
  friend ShardedPairCollection<K1, V1> make_sharded_pair_collection(
      uint32_t max_shard_bytes, uint32_t max_batch_bytes);
  template <typename K1, typename V1>
  friend ShardedPairCollection<K1, V1> make_sharded_pair_collection(
      uint64_t num, K1 estimated_min_key,
      std::function<void(K1 &, uint64_t)> key_inc_fn, uint32_t max_shard_bytes,
      uint32_t max_batch_bytes);
};

template <typename K, typename V>
ShardedPairCollection<K, V> make_sharded_pair_collection(
    uint32_t max_shard_bytes =
        ShardedPairCollection<K, V>::kDefaultMaxShardBytes,
    uint32_t max_batch_bytes =
        ShardedPairCollection<K, V>::kDefaultMaxBatchBytes);

template <typename K, typename V>
ShardedPairCollection<K, V> make_sharded_pair_collection(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn,
    uint32_t max_shard_bytes =
        ShardedPairCollection<K, V>::kDefaultMaxShardBytes,
    uint32_t max_batch_bytes =
        ShardedPairCollection<K, V>::kDefaultMaxBatchBytes);

}  // namespace nu

#include "nu/impl/sharded_pair_collect.ipp"
