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
class ShardedVector {
 public:
  constexpr static uint32_t kDefaultPowerShardSize = 20;

  ShardedVector();
  ShardedVector(const ShardedVector &);
  ShardedVector &operator=(const ShardedVector &);
  ShardedVector(ShardedVector &&);
  ShardedVector &operator=(ShardedVector &&);

  T operator[](uint32_t index);

  void push_back_sync(const T &value);
  void push_back(const T &value);
  void pop_back_sync();
  void pop_back();
  void flush();
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
  template <typename T1>
  class Shard {
   public:
    Shard();
    Shard(size_t capacity, uint32_t size_max, uint32_t initial_size = 0);
    Shard(std::vector<T1> elems, uint32_t size_max);

    T1 operator[](uint32_t index);
    void push_back(const T1 &value);
    void push_back_batch(std::vector<T1> elems);
    void pop_back();
    template <typename T2>
    void set(uint32_t index, T2 &&value);
    template <typename... A0s, typename... A1s>
    void apply(uint32_t index, void (*fn)(T1 &, A0s...), A1s &&... args);
    void clear();
    size_t capacity() const;
    void reserve(size_t new_cap);
    void resize(size_t count);
    std::vector<T1> collect();
    template <typename... A0s, typename... A1s>
    void for_all(T1 (*fn)(T1, A0s...), A1s &&... args);
    template <typename... A0s, typename... A1s>
    void for_all(void (*fn)(T1 &, A0s...), A1s &&... args);
    template <typename RetT, typename... A0s, typename... A1s>
    RetT reduce(RetT initial_val, RetT (*reducer)(RetT, T1, A0s...),
                A1s &&... args);
    template <typename RetT, typename... A0s, typename... A1s>
    RetT reduce(RetT initial_val, void (*reducer)(RetT &, T1 &, A0s...),
                A1s &&... args);

   private:
    std::vector<T1> data_;
    uint32_t size_max_;
  };

  uint32_t shard_max_size_;
  uint32_t shard_max_size_bytes_;
  size_t size_;
  size_t capacity_;
  uint32_t max_tail_buffer_size_;
  std::vector<T> tail_buffer_;
  uint32_t buffered_shard_idx_;
  std::vector<T> read_buffer_;
  std::vector<Proclet<Shard<T>>> shards_;

  struct ElemIndex {
    bool in_buffer;
    union {
      struct shard_idx_t {
        uint32_t shard_idx;
        uint32_t idx_in_shard;
      } shard;
      struct buffer_idx_t {
        uint32_t idx;
      } buffer;
    } loc;
  };
  ElemIndex calc_index(uint32_t index);

  void _resize_down(size_t target_size);
  void _resize_up(size_t target_size);
  void _invalidate_read_buffer();
  template <typename V, typename... A0s, typename... A1s>
  std::vector<V> __for_all_shards(V (*fn)(Shard<T> &, A0s...), A1s &&... args);
  template <typename... A0s, typename... A1s>
  void __for_all_shards(void (*fn)(Shard<T> &, A0s...), A1s &&... args);

  template <typename X>
  friend ShardedVector<X> make_sharded_vector(uint32_t power_shard_sz,
                                              size_t capacity);
};

template <typename T>
ShardedVector<T> make_sharded_vector(
    uint32_t power_shard_sz = ShardedVector<T>::kDefaultPowerShardSize,
    size_t remote_capacity = 0);
}  // namespace nu

#include "nu/impl/sharded_vector.ipp"
