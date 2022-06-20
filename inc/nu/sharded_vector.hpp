#pragma once

#include <thread.h>

#include <cereal/types/vector.hpp>
#include <memory>
#include <utility>
#include <vector>

extern "C" {
#include <runtime/net.h>
}

#include "nu/proclet.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/spin_lock.hpp"

namespace nu {
template <typename T>
class VectorShard;

template <typename T>
class ShardedVector;

template <typename T>
class VectorShard {
 public:
  VectorShard();
  VectorShard(size_t capacity, uint32_t size_max);

  T operator[](uint32_t index);
  void push_back(const T &value);
  void pop_back();
  template <typename T1>
  void set(uint32_t index, T1 &&value);
  template <typename... A0s, typename... A1s>
  void apply(uint32_t index, void (*fn)(T &, A0s...), A1s &&... args);
  void clear();
  size_t capacity() const;
  void reserve(size_t new_cap);
  void resize(size_t count);
  template <typename... A0s, typename... A1s>
  void for_all(T (*fn)(T, A0s...), A1s &&... args);
  template <typename... A0s, typename... A1s>
  void for_all(void (*fn)(T &, A0s...), A1s &&... args);
  template <typename RetT, typename... A0s, typename... A1s>
  RetT reduce(RetT initial_val, RetT (*reducer)(RetT, T, A0s...),
              A1s &&... args);
  template <typename RetT, typename... A0s, typename... A1s>
  RetT reduce(RetT initial_val, void (*reducer)(RetT &, T &, A0s...),
              A1s &&... args);

  template <typename T1>
  friend class ElRef;
  template <typename T1>
  friend class ShardedVector;

 private:
  std::vector<T> data_;
  uint32_t size_max_;
};

template <typename T>
class ShardedVector {
 public:
  constexpr static uint32_t kDefaultPowerShardSize = 20;

  ShardedVector();
  ShardedVector(const ShardedVector &);
  ShardedVector &operator=(const ShardedVector &);
  ShardedVector(ShardedVector &&);
  ShardedVector &operator=(ShardedVector &&);

  T operator[](uint32_t index);

  void push_back(const T &value);
  void pop_back();
  template <typename T1>
  void set(uint32_t index, T1 &&value);
  template <typename... A0s, typename... A1s>
  void apply(uint32_t index, void (*fn)(T &, A0s...), A1s &&... args);
  template <typename... A0s, typename... A1s>
  Future<void> apply_async(uint32_t index, void (*fn)(T &, A0s...),
                           A1s &&... args);
  constexpr bool empty() const noexcept;
  constexpr size_t size() const noexcept;
  void clear();
  size_t capacity();
  void shrink_to_fit();
  void reserve(size_t new_cap);
  void resize(size_t count);
  template <typename... A0s, typename... A1s>
  ShardedVector &for_all(T (*fn)(T, A0s...), A1s &&... args);
  template <typename... A0s, typename... A1s>
  ShardedVector &for_all(void (*fn)(T &, A0s...), A1s &&... args);
  template <typename RetT, typename... A0s, typename... A1s>
  RetT reduce(RetT initial_val, RetT (*reducer)(RetT, T, A0s...),
              A1s &&... args);
  template <typename RetT, typename... A0s, typename... A1s>
  RetT reduce(RetT initial_val, void (*reducer)(RetT &, T &, A0s...),
              A1s &&... args);
  std::vector<T> collect();

  template <class Archive>
  void serialize(Archive &ar);

 private:
  uint32_t shard_max_size_;
  uint32_t shard_max_size_bytes_;
  size_t size_;
  size_t capacity_;
  std::vector<Proclet<VectorShard<T>>> shards_;

  struct ElemIndex {
    uint32_t shard_idx;
    uint32_t idx_in_shard;
  };
  ElemIndex calc_index(uint32_t index);

  void _resize_down(size_t target_size);
  void _resize_up(size_t target_size);
  template <typename V, typename... A0s, typename... A1s>
  std::vector<V> __for_all_shards(V (*fn)(VectorShard<T> &, A0s...),
                                  A1s &&... args);
  template <typename... A0s, typename... A1s>
  void __for_all_shards(void (*fn)(VectorShard<T> &, A0s...), A1s &&... args);

  template <typename X>
  friend ShardedVector<X> make_dis_vector(uint32_t power_shard_sz,
                                          size_t capacity);
};

template <typename T>
ShardedVector<T> make_dis_vector(
    uint32_t power_shard_sz = ShardedVector<T>::kDefaultPowerShardSize,
    size_t capacity = 0);
}  // namespace nu

#include "nu/impl/sharded_vector.ipp"
