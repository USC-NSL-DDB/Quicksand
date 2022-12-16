#pragma once

#include <functional>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "nu/sharded_ds.hpp"

namespace nu {

template <typename K, typename V>
struct PartitionerConstIterator;

template <typename K, typename V>
struct PartitionerConstReverseIterator;

template <typename K, typename V>
class Partitioner {
 public:
  using Key = K;
  using Val = V;
  using DataEntry = std::conditional_t<HasVal<Partitioner>, std::pair<K, V>, K>;
  using ConstIterator = PartitionerConstIterator<K, V>;
  using ConstReverseIterator = PartitionerConstReverseIterator<K, V>;

  Partitioner();
  Partitioner(const Partitioner &);
  Partitioner &operator=(const Partitioner &);
  Partitioner(Partitioner &&) noexcept;
  Partitioner &operator=(Partitioner &&) noexcept;
  ~Partitioner();
  std::size_t size() const;
  std::size_t capacity() const;
  void reserve(std::size_t capacity);
  bool empty() const;
  void clear();
  std::size_t insert(K k, V v) requires HasVal<Partitioner>;
  std::size_t insert(K k) requires(!HasVal<Partitioner>);
  void split(K *mid_k, Partitioner *latter_half);
  void merge(Partitioner partitioner);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const K &key, V &val, S0s...),
               S1s &&... states) requires HasVal<Partitioner>;
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const K &key, S0s...),
               S1s &&... states) requires(!HasVal<Partitioner>);
  ConstIterator find_by_order(std::size_t order);
  ConstIterator cbegin() const;
  ConstIterator cend() const;
  ConstReverseIterator crbegin() const;
  ConstReverseIterator crend() const;
  void sort();
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

  DataEntry *data();

 private:
  DataEntry *data_;
  std::size_t size_;
  std::size_t capacity_;
  bool ownership_;

  void expand();
  void destroy();
};

template <typename K, typename V>
using PartitionerContainer = GeneralLockedContainer<Partitioner<K, V>>;

template <typename K, typename V = ErasedType>
class ShardedPartitioner
    : public ShardedDataStructure<
          PartitionerContainer<K, V>,
          /* LL = */ std::false_type>  // Doesn't make sense to use this data
                                       // structure for any low-latency purpose.
{
 public:
  ShardedPartitioner(const ShardedPartitioner &) = default;
  ShardedPartitioner &operator=(const ShardedPartitioner &) = default;
  ShardedPartitioner(ShardedPartitioner &&) noexcept = default;
  ShardedPartitioner &operator=(ShardedPartitioner &&) noexcept = default;

 private:
  using Base =
      ShardedDataStructure<PartitionerContainer<K, V>, std::false_type>;

  ShardedPartitioner() = default;
  ShardedPartitioner(std::optional<typename Base::Hint> hint);

  friend class ProcletServer;
  template <typename K1, typename V1>
  friend ShardedPartitioner<K1, V1> make_sharded_partitioner();
  template <typename K1, typename V1>
  friend ShardedPartitioner<K1, V1> make_sharded_partitioner(
      uint64_t num, K1 estimated_min_key,
      std::function<void(K1 &, uint64_t)> key_inc_fn);
};

template <typename K, typename V>
struct PartitionerConstIterator
    : public std::span<const typename Partitioner<K, V>::DataEntry>::iterator {
  constexpr static bool kContiguous = true;

  PartitionerConstIterator();
  PartitionerConstIterator(
      std::span<const typename Partitioner<K, V>::DataEntry>::iterator &&iter);
};

template <typename K, typename V>
struct PartitionerConstReverseIterator
    : public std::span<
          const typename Partitioner<K, V>::DataEntry>::reverse_iterator {
  constexpr static bool kContiguous = true;

  PartitionerConstReverseIterator();
  PartitionerConstReverseIterator(
      std::span<const typename Partitioner<K, V>::DataEntry>::reverse_iterator
          &&iter);
};

template <typename K, typename V = ErasedType>
ShardedPartitioner<K, V> make_sharded_partitioner();

template <typename K, typename V = ErasedType>
ShardedPartitioner<K, V> make_sharded_partitioner(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn);

}  // namespace nu

#include "nu/impl/sharded_partitioner.ipp"
