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

  VectorShard();
  VectorShard(std::size_t capacity);
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
};

template <typename T>
class ShardedVector
    : public ShardedDataStructure<GeneralContainer<VectorShard<T>>> {
 public:
  ShardedVector() = default;
  ShardedVector(const ShardedVector &) = default;
  ShardedVector &operator=(const ShardedVector &) = default;
  ShardedVector(ShardedVector &&) noexcept = default;
  ShardedVector &operator=(ShardedVector &&) noexcept = default;

  T operator[](std::size_t index);
  void push_back(const T &value);
  void pop_back();
  template <typename T1>
  void set(std::size_t index, T1 &&value);
  void flush();
  std::size_t size();
  bool empty();

 private:
  template <typename T1>
  friend ShardedVector<T1> make_sharded_vector(uint32_t power_shard_sz);
};

template <typename T>
ShardedVector<T> make_sharded_vector(uint32_t power_shard_sz);
}  // namespace nu

#include "nu/impl/sharded_vector.ipp"
