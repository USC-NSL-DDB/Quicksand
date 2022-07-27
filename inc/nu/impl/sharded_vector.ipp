#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>
#include <cstdint>
#include <iterator>

#include "nu/commons.hpp"

namespace nu {
template <typename T>
VectorShard<T>::VectorShard()
    : data_(0), capacity_(0), l_key_(SIZE_MAX), r_key_(SIZE_MAX) {}

template <typename T>
VectorShard<T>::VectorShard(
    const GeneralShard<GeneralContainer<VectorShard>> &shard)
    : data_(0), capacity_(0), l_key_(shard.l_key_), r_key_(shard.r_key_) {}

template <typename T>
VectorShard<T>::VectorShard(std::size_t capacity)
    : data_(0), capacity_(capacity), l_key_(SIZE_MAX), r_key_(SIZE_MAX) {}

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
      r_key_(o.r_key_) {
  o.data_.clear();
  l_key_ = SIZE_MAX;
  r_key_ = SIZE_MAX;
}

template <typename T>
VectorShard<T> &VectorShard<T>::operator=(VectorShard &&o) noexcept {
  data_ = o.data_;
  capacity_ = o.capacity_;
  l_key_ = o.l_key_;
  r_key_ = o.r_key_;
  o.data_.clear();
  l_key_ = SIZE_MAX;
  r_key_ = SIZE_MAX;
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
}

template <typename T>
void VectorShard<T>::emplace(Key k, Val v) {
  BUG_ON(l_key_ == SIZE_MAX || r_key_ == SIZE_MAX);
  BUG_ON(k < l_key_ || k > r_key_);
  BUG_ON(data_.size() == capacity_);
  std::size_t idx = k - l_key_;

  if (idx == data_.size()) {
    data_.push_back(v);
  } else {
    data_[idx] = v;
  }
}

template <typename T>
void VectorShard<T>::emplace_batch(VectorShard &&shard) {
  BUG_ON(data_.size() + shard.size() > capacity_);
  data_.insert(data_.end(), shard.data_.begin(), shard.data_.end());
}

template <typename T>
std::pair<typename VectorShard<T>::Key, VectorShard<T>>
VectorShard<T>::split() {
  std::size_t r_key_new = r_key_ / 2;
  BUG_ON(r_key_new == r_key_);
  std::size_t capacity_new = capacity_ / 2;

  VectorShard<T> new_shard;
  new_shard.l_key_ = r_key_new + 1;
  new_shard.r_key_ = r_key_;
  new_shard.capacity_ = capacity_ - capacity_new;
  if (data_.size() > capacity_new) {
    new_shard.data_ =
        std::vector(std::make_move_iterator(data_.begin() + capacity_new),
                    std::make_move_iterator(data_.end()));
    data_.resize(capacity_new);
  }

  r_key_ = r_key_new;
  capacity_ = capacity_new;

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
  // TODO
}

template <typename T>
void ShardedVector<T>::push_back(const T &value) {
  // TODO
}

template <typename T>
void ShardedVector<T>::pop_back() {
  // TODO
}

template <typename T>
template <typename T1>
void ShardedVector<T>::set(std::size_t index, T1 &&value) {
  // TODO
}

template <typename T>
std::size_t ShardedVector<T>::size() {
  return size_;
}

template <typename T>
bool ShardedVector<T>::empty() {
  return size_ == 0;
}

}  // namespace nu
