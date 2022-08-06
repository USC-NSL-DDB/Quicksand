#pragma once

#include <vector>

#include "sharded_ds.hpp"

namespace nu {

template <typename T>
class Vector {
 public:
  using Key = std::size_t;
  using Val = T;

  Vector();
  Vector(std::size_t capacity);
  Vector(const Vector &);
  Vector &operator=(const Vector &);
  Vector(Vector &&) noexcept;
  Vector &operator=(Vector &&) noexcept;

  std::size_t size() const;
  std::size_t capacity() const;
  bool empty() const;
  void clear();
  void emplace(Key k, Val v);
  void emplace_batch(Vector &&vector);
  std::optional<T> find(Key k);
  std::pair<Key, Vector> split();
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  std::vector<T> data_;
  Key l_key_;
};

template <typename T>
class ShardedVector
    : public ShardedDataStructure<GeneralContainer<Vector<T>>> {
 public:
  constexpr static uint32_t kDefaultMaxShardBytes = 16 << 20;
  constexpr static uint32_t kDefaultMaxBatchBytes = 16 << 10;

  ShardedVector();
  ShardedVector(const ShardedVector &);
  ShardedVector &operator=(const ShardedVector &);
  ShardedVector(ShardedVector &&) noexcept;
  ShardedVector &operator=(ShardedVector &&) noexcept;

  T operator[](std::size_t index);
  void push_back(const T &value);
  void pop_back();
  std::size_t size();
  bool empty();
  void clear();

 private:
  using Base = ShardedDataStructure<GeneralContainer<Vector<T>>>;
  ShardedVector(uint32_t max_shard_bytes, uint32_t max_batch_bytes);

  std::size_t size_;

  template <typename T1>
  friend ShardedVector<T1> make_sharded_vector(uint32_t max_shard_bytes,
                                               uint32_t max_batch_bytes);
};

template <typename T>
ShardedVector<T> make_sharded_vector(
    uint32_t max_shard_bytes = ShardedVector<T>::kDefaultMaxShardBytes,
    uint32_t max_batch_bytes = ShardedVector<T>::kDefaultMaxBatchBytes);
}  // namespace nu

#include "nu/impl/sharded_vector.ipp"
