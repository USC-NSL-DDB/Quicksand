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
#include "nu/utils/spinlock.hpp"

namespace nu {
template <typename T>
class ArrayShard {
 public:
  ArrayShard(uint32_t capacity);

  T operator[](uint32_t index);

 private:
  uint32_t size_;
  std::vector<T> data_;
};

template <typename T>
class DistributedArray {
 public:
  constexpr static uint32_t kDefaultPowerShardSize = 20;

  DistributedArray();
  DistributedArray(const DistributedArray &);
  DistributedArray &operator=(const DistributedArray &);
  DistributedArray(DistributedArray &&);
  DistributedArray &operator=(DistributedArray &&);

  T operator[](uint32_t index);

 private:
  uint32_t power_shard_sz_;
  uint32_t shard_sz_;
  uint32_t elems_per_shard_;
  uint32_t size_;
  std::vector<Proclet<ArrayShard<T>>> shards_;

  template <typename X>
  friend DistributedArray<X> make_dis_array(uint32_t size,
                                            uint32_t power_shard_sz);
};

template <typename T>
DistributedArray<T> make_dis_array(
    uint32_t size,
    uint32_t power_shard_sz = DistributedArray<T>::kDefaultPowerShardSize);
}  // namespace nu

#include "nu/impl/dis_array.ipp"
