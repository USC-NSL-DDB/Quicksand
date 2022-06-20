#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>

#include "nu/commons.hpp"

namespace nu {
template <typename T>
ArrayShard<T>::ArrayShard(uint32_t capacity) : size_(capacity) {
  data_.reserve(capacity);
}

template <typename T>
T ArrayShard<T>::operator[](uint32_t index) {
  return data_[index];
}

template <typename T>
void ArrayShard<T>::set(uint32_t index, T value) {
  data_[index] = value;
}

template <typename T>
ShardedArray<T>::ShardedArray(const ShardedArray& o) {
  *this = o;
}

template <typename T>
ShardedArray<T>& ShardedArray<T>::operator=(const ShardedArray& o) {
  power_shard_sz_ = o.power_shard_sz_;
  shard_sz_ = o.shard_sz_;
  shards_ = o.shards_;
  elems_per_shard_ = o.elems_per_shard_;
  size_ = o.size_;
  return *this;
}

template <typename T>
ShardedArray<T>::ShardedArray(ShardedArray&& o) {
  *this = std::move(o);
}

template <typename T>
ShardedArray<T>& ShardedArray<T>::operator=(ShardedArray&& o) {
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
ShardedArray<T>::ShardedArray()
    : power_shard_sz_(0), shard_sz_(0), elems_per_shard_(0), size_(0) {}

template <typename T>
T ShardedArray<T>::operator[](uint32_t index) {
  uint32_t shard_idx = index / elems_per_shard_;
  uint32_t idx_in_shard = index % elems_per_shard_;
  auto& shard = shards_[shard_idx];
  return shard.run(
      +[](ArrayShard<T>& shard, uint32_t idx) { return shard[idx]; },
      idx_in_shard);
}

template <typename T>
void ShardedArray<T>::set(uint32_t index, T value) {
  if (index >= size_) return;
  uint32_t shard_idx = index / elems_per_shard_;
  uint32_t idx_in_shard = index % elems_per_shard_;
  auto& shard = shards_[shard_idx];
  shard.run(
      +[](ArrayShard<T>& shard, uint32_t idx, T value) {
        shard.set(idx, value);
      },
      idx_in_shard, value);
}

template <typename T>
template <class Archive>
void ShardedArray<T>::serialize(Archive& ar) {
  ar(power_shard_sz_);
  ar(shard_sz_);
  ar(elems_per_shard_);
  ar(size_);
  ar(shards_);
}

template <typename T>
ShardedArray<T> make_dis_array(uint32_t size, uint32_t power_shard_sz) {
  ShardedArray<T> arr;

  arr.power_shard_sz_ = power_shard_sz;
  arr.shard_sz_ = (1 << power_shard_sz);
  arr.size_ = size;

  BUG_ON(arr.shard_sz_ < sizeof(T));

  uint32_t num_shards =
      ((sizeof(T) * size) + arr.shard_sz_ - 1) / arr.shard_sz_;
  arr.elems_per_shard_ = arr.shard_sz_ / sizeof(T);

  for (uint32_t i = 0; i < num_shards - 1; i++) {
    arr.shards_.emplace_back(make_proclet<ArrayShard<T>>(arr.elems_per_shard_));
  }

  arr.shards_.emplace_back(make_proclet<ArrayShard<T>>(
      size - arr.elems_per_shard_ * (num_shards - 1)));

  return arr;
}
}  // namespace nu
