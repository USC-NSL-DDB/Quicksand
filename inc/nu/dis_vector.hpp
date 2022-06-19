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
class DistributedVector;

template <typename T>
class ElRef {
 public:
  ElRef();
  ElRef(uint32_t index, T elem);
  ElRef &operator=(const T &);
  template <typename T1>
  bool operator==(T1 &&);
  const T &operator*();

  template <class Archive>
  void serialize(Archive &ar);

  template <typename T1>
  friend class DistributedVector;

 private:
  T el_;
  uint32_t idx_;
  std::optional<WeakProclet<VectorShard<T>>> shard_;
};

template <typename T>
class VectorShard {
 public:
  VectorShard();
  VectorShard(size_t capacity, uint32_t size_max);

  ElRef<T> operator[](uint32_t index);
  void push_back(const T &value);
  void pop_back();
  void clear();
  size_t capacity() const;
  void reserve(size_t new_cap);
  void resize(size_t count);
  void transform(T (*fn)(T));
  void transform(void (*fn)(T &));

  template <typename T1>
  friend class ElRef;

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

  ElRef<T> operator[](uint32_t index);

  void push_back(const T &value);
  void pop_back();
  constexpr bool empty() const noexcept;
  constexpr size_t size() const noexcept;
  void clear();
  size_t capacity();
  void shrink_to_fit();
  void reserve(size_t new_cap);
  void resize(size_t count);
  DistributedVector &transform(T (*fn)(T));
  DistributedVector &transform(void (*fn)(T &));

  template <class Archive>
  void serialize(Archive &ar);

 private:
  uint32_t shard_max_size_;
  uint32_t shard_max_size_bytes_;
  size_t size_;
  size_t capacity_;
  std::vector<Proclet<VectorShard<T>>> shards_;

  void _resize_down(size_t target_size);
  void _resize_up(size_t target_size);

  template <typename X>
  friend DistributedVector<X> make_dis_vector(uint32_t power_shard_sz,
                                              size_t capacity);
};

template <typename T>
DistributedVector<T> make_dis_vector(
    uint32_t power_shard_sz = DistributedVector<T>::kDefaultPowerShardSize,
    size_t capacity = 0);
}  // namespace nu

#include "nu/impl/dis_vector.ipp"
