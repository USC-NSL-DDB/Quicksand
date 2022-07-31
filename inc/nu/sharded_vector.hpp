#pragma once

#include <thread.h>

#include <cereal/types/vector.hpp>
#include <memory>
#include <utility>
#include <vector>

#include "sharded_ds.hpp"

extern "C" {
#include <runtime/net.h>
}

#include "nu/proclet.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/spin_lock.hpp"

namespace nu {

template <typename T>
class VectorShard {
 public:
  using Key = std::size_t;
  using Val = T;
  using Shard = GeneralShard<GeneralContainer<VectorShard<T>>>;

  VectorShard();
  VectorShard(std::size_t capacity);
  VectorShard(const Shard *s, std::size_t capacity);
  VectorShard(const VectorShard &);
  VectorShard &operator=(const VectorShard &);
  VectorShard(VectorShard &&) noexcept;
  VectorShard &operator=(VectorShard &&) noexcept;

  std::size_t size() const;
  std::size_t capacity() const;
  bool empty() const;
  void clear();
  void emplace(Key k, Val v);
  void emplace_batch(VectorShard &&shard);
  std::optional<T> find(Key k);
  std::pair<Key, VectorShard> split();
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(std::pair<const Key, Val> &, S0s...),
               S1s &&... states);
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  std::vector<T> data_;
  std::size_t capacity_;
  std::size_t l_key_;
  std::size_t r_key_;
};

template <typename T>
class ShardedVector
    : public ShardedDataStructure<GeneralContainer<VectorShard<T>>> {
 public:
  constexpr static uint32_t kDefaultMaxShardBytes = 16 << 20;
  constexpr static uint32_t kDefaultMaxBatchBytes = 100 << 10;

  ShardedVector();
  ShardedVector(const ShardedVector &);
  ShardedVector &operator=(const ShardedVector &);
  ShardedVector(ShardedVector &&) noexcept;
  ShardedVector &operator=(ShardedVector &&) noexcept;

  T operator[](std::size_t index);
  void push_back(const T &value);
  void pop_back();
  template <typename T1>
  void set(std::size_t index, T1 &&value);
  std::size_t size();
  bool empty();

 private:
  using Base = ShardedDataStructure<GeneralContainer<VectorShard<T>>>;
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
