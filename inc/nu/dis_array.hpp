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
  ArrayShard(usize capacity);

  T operator[](usize index);

 private:
  usize size_;
  std::vector<T> data_;
}

template <typename T>
class DistributedArray {
  constexpr static uint32_t kDefaultPowerShardSize = 20;

  DistributedArray(const DistributedArray &);
  DistributedArray &operator=(const DistributedArray &);
  DistributedArray(DistributedArray &&);
  DistributedArray &operator=(DistributedArray &&);

  T operator[](usize index);

 private:
  uint32_t power_shard_sz_;
  uint32_t shard_sz_;
  usize elems_per_shard_;
  usize size_;
  std::vector<Proclet<ArrayShard>> shards_;
}

template <typename T>
DistributedArray<T> make_dis_array(
    usize size,
    uint32_t power_shard_sz_ = DistributedArray<T, N>::kDefaultPowerShardSize);
}  // namespace nu
