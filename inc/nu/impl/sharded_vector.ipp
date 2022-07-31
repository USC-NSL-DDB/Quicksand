#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>
#include <cstdint>
#include <iterator>
#include <optional>

#include "nu/commons.hpp"

namespace nu {
template <typename T>
VectorShard<T>::VectorShard()
    : data_(0), capacity_(0), l_key_(SIZE_MAX), r_key_(SIZE_MAX) {}

template <typename T>
VectorShard<T>::VectorShard(std::size_t capacity)
    : data_(0), capacity_(capacity), l_key_(SIZE_MAX), r_key_(SIZE_MAX) {}

template <typename T>
VectorShard<T>::VectorShard(const VectorShard<T>::Shard *shard,
                            std::size_t capacity)
    : data_(0), capacity_(capacity), l_key_(SIZE_MAX), r_key_(SIZE_MAX) {
  if (shard) {
    l_key_ = shard->l_key().value_or(SIZE_MAX);
    r_key_ = shard->r_key().value_or(SIZE_MAX);
  }
}

template <typename T>
VectorShard<T>::VectorShard(const VectorShard &o) {
  *this = o;
}

template <typename T>
VectorShard<T> &VectorShard<T>::operator=(const VectorShard &o) {
  data_ = o.data_;
  capacity_ = o.capacity_;
  l_key_ = o.l_key_;
  r_key_ = o.r_key_;
  return *this;
}

template <typename T>
VectorShard<T>::VectorShard(VectorShard &&o) noexcept
    : data_(o.data_),
      capacity_(o.capacity_),
      l_key_(o.l_key_),
      r_key_(o.r_key_) {}

template <typename T>
VectorShard<T> &VectorShard<T>::operator=(VectorShard &&o) noexcept {
  data_ = o.data_;
  capacity_ = o.capacity_;
  l_key_ = o.l_key_;
  r_key_ = o.r_key_;
  return *this;
}

template <typename T>
std::size_t VectorShard<T>::size() const {
  return data_.size();
}

template <typename T>
std::size_t VectorShard<T>::capacity() const {
  return capacity_;
}

template <typename T>
bool VectorShard<T>::empty() const {
  return data_.empty();
}

template <typename T>
void VectorShard<T>::clear() {
  data_.clear();
  l_key_ = SIZE_MAX;
  r_key_ = SIZE_MAX;
}

template <typename T>
void VectorShard<T>::emplace(Key k, Val v) {
  if (l_key_ == SIZE_MAX) {
    l_key_ = k;
  }
  if (k < l_key_ || k > r_key_) {
    BUG_ON(k < l_key_ || k > r_key_);
  }
  std::size_t idx = k - l_key_;

  if (idx == data_.size()) {
    data_.push_back(v);
  } else if (idx < data_.size()) {
    data_[idx] = v;
  } else {
    BUG();
  }
}

template <typename T>
void VectorShard<T>::emplace_batch(VectorShard &&shard) {
  BUG_ON(data_.size() + shard.size() > capacity_);
  data_.insert(data_.end(), shard.data_.begin(), shard.data_.end());
}

template <typename T>
std::optional<T> VectorShard<T>::find(std::size_t k) {
  if (k < l_key_ || k > r_key_) {
    return std::nullopt;
  }
  std::size_t idx = k - l_key_;
  if (idx >= data_.size()) {
    return std::nullopt;
  }
  return std::optional{data_[idx]};
}

template <typename T>
std::pair<typename VectorShard<T>::Key, VectorShard<T>>
VectorShard<T>::split() {
  BUG_ON(data_.size() == 0);
  r_key_ = data_.size() - 1;
  VectorShard<T> new_shard;
  new_shard.l_key_ = data_.size();
  return {new_shard.l_key_, new_shard};
}

template <typename T>
template <typename... S0s, typename... S1s>
void VectorShard<T>::for_all(void (*fn)(std::pair<const Key, Val> &, S0s...),
                             S1s &&... states) {
  for (std::size_t i = 0; i < data_.size(); i++) {
    std::pair<const std::size_t, T> p = {i, data_[i]};
    fn(p, std::forward<S0s>(states)...);
  }
}

template <typename T>
template <class Archive>
void VectorShard<T>::save(Archive &ar) const {
  ar(data_);
  ar(capacity_);
  ar(l_key_);
  ar(r_key_);
}

template <typename T>
template <class Archive>
void VectorShard<T>::load(Archive &ar) {
  ar(data_);
  ar(capacity_);
  ar(l_key_);
  ar(r_key_);
}

template <typename T>
ShardedVector<T>::ShardedVector() : size_(0) {}

template <typename T>
ShardedVector<T>::ShardedVector(const ShardedVector &o) {
  *this = o;
}

template <typename T>
ShardedVector<T> &ShardedVector<T>::operator=(const ShardedVector &o) {
  size_ = o.size_;
  return *this;
}

template <typename T>
ShardedVector<T>::ShardedVector(ShardedVector &&o) noexcept {
  *this = std::move(o);
}

template <typename T>
ShardedVector<T> &ShardedVector<T>::operator=(ShardedVector &&o) noexcept {
  size_ = o.size_;
  return *this;
}

template <typename T>
T ShardedVector<T>::operator[](std::size_t index) {
  BUG_ON(index >= size_);
  std::optional<T> r = this->find(index);
  assert(r.has_value());
  return r.value();
}

template <typename T>
void ShardedVector<T>::push_back(const T &value) {
  this->emplace(size_, value);
  size_++;
}

template <typename T>
void ShardedVector<T>::pop_back() {
  size_--;
}

template <typename T>
template <typename T1>
void ShardedVector<T>::set(std::size_t index, T1 &&value) {
  this->emplace(index, value);
}

template <typename T>
std::size_t ShardedVector<T>::size() {
  return size_;
}

template <typename T>
bool ShardedVector<T>::empty() {
  return size_ == 0;
}

template <typename T>
ShardedVector<T>::ShardedVector(uint32_t max_shard_bytes,
                                uint32_t max_batch_bytes)
    : Base(0, std::nullopt, max_shard_bytes, max_batch_bytes), size_(0) {}

template <typename T>
ShardedVector<T> make_sharded_vector(uint32_t max_shard_bytes,
                                     uint32_t max_batch_bytes) {
  return ShardedVector<T>(max_shard_bytes, max_batch_bytes);
}

}  // namespace nu
