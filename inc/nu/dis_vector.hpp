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
class VectorShard {
 public:
  VectorShard();
  VectorShard(uint32_t capacity, uint32_t size_max);

  T operator[](uint32_t index);
  void push_back(const T &value);

 private:
  std::vector<T> data_;
  uint32_t size_max_;
};

template <typename T>
class DistributedVector {
 public:
  constexpr static uint32_t kDefaultPowerShardSize = 20;

  DistributedVector();
  DistributedVector(const DistributedVector &);
  DistributedVector &operator=(const DistributedVector &);
  DistributedVector(DistributedVector &&);
  DistributedVector &operator=(DistributedVector &&);

  T operator[](uint32_t index);

  void set(uint32_t index, T value);
  void push_back(const T &value);

  template <class Archive>
  void serialize(Archive &ar);

 private:
  uint32_t shard_max_size_;
  uint32_t shard_max_size_bytes_;
  uint32_t size_;
  std::vector<Proclet<VectorShard<T>>> shards_;

  template <typename X>
  friend DistributedVector<X> make_dis_vector(uint32_t power_shard_sz);
};

template <typename T>
DistributedVector<T> make_dis_vector(
    uint32_t power_shard_sz = DistributedVector<T>::kDefaultPowerShardSize);
}  // namespace nu

#include "nu/impl/dis_vector.ipp"
