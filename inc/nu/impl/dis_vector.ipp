#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>

#include "nu/commons.hpp"

namespace nu {
template <typename T>
ElRef<T>::ElRef() {}

template <typename T>
ElRef<T>::ElRef(uint32_t index, T element) : el_(element), idx_(index) {}

template <typename T>
const T& ElRef<T>::operator*() {
  return el_;
}

template <typename T>
template <typename T1>
bool ElRef<T>::operator==(T1&& rhs) {
  return rhs == el_;
}

template <typename T>
ElRef<T>& ElRef<T>::operator=(const T& value) {
  shard_.value().__run(
      +[](VectorShard<T>& shard, uint32_t idx, const T& value) {
        shard.data_[idx] = value;
      },
      idx_, value);
  return *this;
}

template <typename T>
template <class Archive>
void ElRef<T>::serialize(Archive& ar) {
  ar(el_);
  ar(idx_);
  ar(shard_);
}

template <typename T>
VectorShard<T>::VectorShard() : data_(0), size_max_(0) {}

template <typename T>
VectorShard<T>::VectorShard(uint32_t capacity, uint32_t size_max)
    : data_(0), size_max_(size_max) {
  if (capacity) {
    data_.reserve(capacity);
  }
}

template <typename T>
ElRef<T> VectorShard<T>::operator[](uint32_t index) {
  return ElRef(index, data_[index]);
}

template <typename T>
void VectorShard<T>::push_back(const T& value) {
  BUG_ON(data_.size() > size_max_);
  data_.push_back(value);
}

template <typename T>
void VectorShard<T>::pop_back() {
  BUG_ON(data_.size() == 0);
  data_.pop_back();
}

template <typename T>
DistributedVector<T>::DistributedVector()
    : shard_max_size_(0), shard_max_size_bytes_(0), size_(0), shards_(0) {}

template <typename T>
DistributedVector<T>::DistributedVector(const DistributedVector& o) {
  *this = o;
}

template <typename T>
DistributedVector<T>& DistributedVector<T>::operator=(
    const DistributedVector& o) {
  shard_max_size_bytes_ = o.shard_max_size_bytes_;
  shard_max_size_ = o.shard_max_size_;
  shards_ = o.shards_;
  size_ = o.size_;
  return *this;
}

template <typename T>
DistributedVector<T>::DistributedVector(DistributedVector&& o) {
  *this = std::move(o);
}

template <typename T>
DistributedVector<T>& DistributedVector<T>::operator=(DistributedVector&& o) {
  shard_max_size_ = o.shard_max_size_;
  shard_max_size_bytes_ = o.shard_max_size_bytes_;
  size_ = o.size_;
  for (uint32_t i = 0; i < o.shards_.size(); i++) {
    shards_.emplace_back(std::move(o.shards_[i]));
  }
  return *this;
}

template <typename T>
ElRef<T> DistributedVector<T>::operator[](uint32_t index) {
  uint32_t shard_idx = index / shard_max_size_;
  uint32_t idx_in_shard = index % shard_max_size_;
  auto& shard = shards_[shard_idx];
  auto ret = shard.__run(
      +[](VectorShard<T>& shard, uint32_t idx) { return shard[idx]; },
      idx_in_shard);
  ret.shard_ = shard.get_weak();
  return ret;
}

template <typename T>
void DistributedVector<T>::set(uint32_t index, T value) {
  if (index >= size_) return;
  uint32_t shard_idx = index / shard_max_size_;
  uint32_t idx_in_shard = index % shard_max_size_;
  auto& shard = shards_[shard_idx];
  shard.__run(
      +[](VectorShard<T>& shard, uint32_t idx, T value) {
        shard.set(idx, value);
      },
      idx_in_shard, value);
}

template <typename T>
void DistributedVector<T>::push_back(const T& value) {
  BUG_ON(shard_max_size_ == 0);
  uint32_t shard_idx = size_ / shard_max_size_;
  BUG_ON(shard_idx > shards_.size());
  if (shard_idx == shards_.size()) {
    uint32_t capacity = shard_max_size_;
    uint32_t max_size = shard_max_size_;
    shards_.emplace_back(make_proclet<VectorShard<T>>(capacity, max_size));
  }
  auto& shard = shards_[shard_idx];
  shard.__run(
      +[](VectorShard<T>& shard, T value) { shard.push_back(value); }, value);
  size_++;
}

template <typename T>
void DistributedVector<T>::pop_back() {
  if (size_ == 0) return;

  BUG_ON(shard_max_size_ == 0);
  uint32_t shard_idx = (size_ - 1) / shard_max_size_;
  BUG_ON(shard_idx >= shards_.size());

  bool last_shard_empty = ((size_ - 1) % shard_max_size_) == 0;
  if (last_shard_empty) {
    shards_.pop_back();
  } else {
    auto& shard = shards_[shard_idx];
    shard.__run(+[](VectorShard<T>& shard) { shard.pop_back(); });
  }
  size_--;
}

template <typename T>
bool DistributedVector<T>::empty() {
  return size_ == 0;
}

template <typename T>
size_t DistributedVector<T>::size() {
  return size_;
}

template <typename T>
template <class Archive>
void DistributedVector<T>::serialize(Archive& ar) {
  ar(shard_max_size_bytes_);
  ar(shard_max_size_);
  ar(size_);
  ar(shards_);
}

template <typename T>
DistributedVector<T> make_dis_vector(uint32_t power_shard_sz) {
  DistributedVector<T> vec;

  vec.shard_max_size_bytes_ = (1 << power_shard_sz);
  vec.shard_max_size_ = vec.shard_max_size_bytes_ / sizeof(T);

  BUG_ON(vec.shard_max_size_bytes_ < sizeof(T));

  return vec;
}
}  // namespace nu
