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
class ArrayShard {
 public:
  ArrayShard(uint32_t capacity);

  T operator[](uint32_t index);
  void set(uint32_t index, T value);

 private:
  uint32_t size_;
  std::vector<T> data_;
};

template <typename T>
class ShardedArray {
 public:
  constexpr static uint32_t kDefaultPowerShardSize = 20;

  ShardedArray();
  ShardedArray(const ShardedArray &);
  ShardedArray &operator=(const ShardedArray &);
  ShardedArray(ShardedArray &&);
  ShardedArray &operator=(ShardedArray &&);

  T operator[](uint32_t index);

  void set(uint32_t index, T value);

  template <class Archive>
  void serialize(Archive &ar);

 private:
  uint32_t power_shard_sz_;
  uint32_t shard_sz_;
  uint32_t elems_per_shard_;
  uint32_t size_;
  std::vector<Proclet<ArrayShard<T>>> shards_;

  template <typename X>
  friend ShardedArray<X> make_sharded_array(uint32_t size,
                                            uint32_t power_shard_sz);
};

template <typename T>
ShardedArray<T> make_sharded_array(
    uint32_t size,
    uint32_t power_shard_sz = ShardedArray<T>::kDefaultPowerShardSize);
}  // namespace nu

#include "nu/impl/sharded_array.ipp"
