#pragma once

#include <functional>
#include <vector>

#include "nu/sharded_ds.hpp"

namespace nu {

template <typename K, typename V>
class PairCollection {
 public:
  using Key = K;
  using Val = V;

  PairCollection();
  PairCollection(const PairCollection &);
  PairCollection &operator=(const PairCollection &);
  PairCollection(PairCollection &&) noexcept;
  PairCollection &operator=(PairCollection &&) noexcept;
  ~PairCollection();
  std::size_t size() const;
  std::size_t capacity() const;
  void reserve(std::size_t capacity);
  bool empty() const;
  void clear();
  void emplace(K k, V v);
  void split(Key *mid_k, PairCollection *latter_half);
  void merge(PairCollection pc);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
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
  void destroy();
};

template <typename K, typename V>
using PairCollectionContainer = GeneralLockedContainer<PairCollection<K, V>>;

template <typename K, typename V>
class ShardedPairCollection
    : public ShardedDataStructure<
          PairCollectionContainer<K, V>,
          /* LL = */ std::false_type>  // Doesn't make sense to use this data
                                       // structure for any low-latency purpose.
{
 public:
  ShardedPairCollection(const ShardedPairCollection &) = default;
  ShardedPairCollection &operator=(const ShardedPairCollection &) = default;
  ShardedPairCollection(ShardedPairCollection &&) noexcept = default;
  ShardedPairCollection &operator=(ShardedPairCollection &&) noexcept = default;

 private:
  using Base =
      ShardedDataStructure<PairCollectionContainer<K, V>, std::false_type>;

  ShardedPairCollection() = default;
  ShardedPairCollection(std::optional<typename Base::Hint> hint);

  friend class ProcletServer;
  template <typename K1, typename V1>
  friend ShardedPairCollection<K1, V1> make_sharded_pair_collection();
  template <typename K1, typename V1>
  friend ShardedPairCollection<K1, V1> make_sharded_pair_collection(
      uint64_t num, K1 estimated_min_key,
      std::function<void(K1 &, uint64_t)> key_inc_fn);
};

template <typename K, typename V>
ShardedPairCollection<K, V> make_sharded_pair_collection();

template <typename K, typename V>
ShardedPairCollection<K, V> make_sharded_pair_collection(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn);

}  // namespace nu

#include "nu/impl/sharded_pair_collect.ipp"
