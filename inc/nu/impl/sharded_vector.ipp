#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>

#include "nu/commons.hpp"

namespace nu {
template <typename T>
VectorShard<T>::VectorShard() : data_(0), capacity_(0) {}

template <typename T>
VectorShard<T>::VectorShard(std::size_t capacity)
    : data_(0), capacity_(capacity) {}

template <typename T>
VectorShard<T>::VectorShard(const VectorShard &o) {
  *this = o;
}

template <typename T>
VectorShard<T> &VectorShard<T>::operator=(const VectorShard &o) {
  data_ = o.data_;
  capacity_ = o.capacity_;
  return *this;
}

template <typename T>
VectorShard<T>::VectorShard(VectorShard &&o) noexcept
    : data_(o.data_), capacity_(o.capacity_) {
  o.data_.clear();
}

template <typename T>
VectorShard<T> &VectorShard<T>::operator=(VectorShard &&o) noexcept {
  data_ = o.data_;
  capacity_ = o.capacity_;
  o.data_.clear();
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
  // TODO
}

template <typename T>
void VectorShard<T>::emplace_batch(VectorShard &&shard) {
  // TODO
}

template <typename T>
std::pair<typename VectorShard<T>::Key, VectorShard<T>>
VectorShard<T>::split() {
  // TODO
}

template <typename T>
template <typename... S0s, typename... S1s>
void VectorShard<T>::for_all(void (*fn)(std::pair<const Key, Val> &, S0s...),
                             S1s &&... states) {
  // TODO
}

template <typename T>
template <class Archive>
void VectorShard<T>::save(Archive &ar) const {
  ar(data_);
  ar(capacity_);
}

template <typename T>
template <class Archive>
void VectorShard<T>::load(Archive &ar) {
  ar(data_);
  ar(capacity_);
}

}  // namespace nu
