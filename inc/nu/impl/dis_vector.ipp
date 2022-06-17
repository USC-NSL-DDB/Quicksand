#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>

#include "nu/commons.hpp"

namespace nu {
template <typename T>
VectorShard<T>::VectorShard() : data_(0) {}

template <typename T>
VectorShard<T>::operator[](uint32_t index) {
  return data_[index];
}

template <typename T>
void set(uint32_t index, T value) {
  data_[index] = value;
}

template <typename T>
DistributedVector::DistributedVector()
    : power_shard_sz_(0),
      shard_sz_(0),
      elems_per_shard(0),
      size_(0),
      shards_(0) {}

template <typename T>
DistributedVector<T>::DistributedVector(const DistributedVector& o) {
  *this = o;
}

template <typename T>
DistributedVector<T>& DistributedVector<T>::operator=(
    const DistributedVector& o) {
  power_shard_sz_ = o.power_shard_sz_;
  shard_sz_ = o.shard_sz_;
  shards_ = o.shards_;
  elems_per_shard_ = o.elems_per_shard_;
  size_ = o.size_;
  return *this;
}

template <typename T>
DistributedVector<T>::DistributedVector(DistributedVector&& o) {
  *this = std::move(o);
}

template <typename T>
DistributedVector<T>& DistributedVector<T>::operator=(DistributedVector&& o) {
  power_shard_sz_ = o.power_shard_sz_;
  shard_sz_ = o.shard_sz_;
  elems_per_shard_ = o.elems_per_shard_;
  size_ = o.size_;
  for (uint32_t i = 0; i < o.shards_.size(); i++) {
    shards_.emplace_back(std::move(o.shards_[i]));
  }
  return *this;
}

template <typename T>
void DistributedVector<T>::operator[](uint32_t index) {
  if (index >= size_) return;
  uint32_t shard_idx = index / elems_per_shard_;
  uint32_t idx_in_shard = index % elems_per_shard_;
  auto& shard = shards_[shard_idx];
  shard.__run(
      +[](VectorShard<T>& shard, uint32_t idx, T value) {
        shard.set(idx, value);
      },
      idx_in_shard, value);
}

template <typename T>
template <class Archive>
void DistributedVector<T>::serialize(Archive& ar) {
  ar(power_shard_sz_);
  ar(shard_sz_);
  ar(elems_per_shard_);
  ar(size_);
  ar(shards_);
}

template <typename T>
DistributedVector<T> make_dis_vector(uint32_t size, uint32_t power_shard_sz) {
  DistributedVector<T> vec;

  vec.power_shard_sz_ = power_shard_sz;
  vec.shard_sz_ = (1 << power_shard_sz);
  vec.size_ = size;

  BUG_ON(vec.shard_sz_ < sizeof(T));

  uint32_t num_shards =
      ((sizeof(T) * size) + vec.shard_sz_ - 1) / vec.shard_sz_;
  vec.elems_per_shard_ = vec.shard_sz_ / sizeof(T);

  for (uint32_t i = 0; i < num_shards - 1; i++) {
    vec.shards_.emplace_back(
        make_proclet<VectorShard<T>>(vec.elems_per_shard_));
  }

  vec.shards_.emplace_back(make_proclet<VectorShard<T>>(
      size - vec.elems_per_shard_ * (num_shards - 1)));

  return vec;
}
}  // namespace nu
