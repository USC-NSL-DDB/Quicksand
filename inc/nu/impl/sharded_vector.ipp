#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>
#include <cstdint>
#include <iterator>
#include <optional>

#include "nu/commons.hpp"

namespace nu {
template <typename T>
VectorShard<T>::VectorShard()
    : data_(0),
      capacity_(SIZE_MAX),
      l_key_inferred_(SIZE_MAX),
      shard_(nullptr) {}

template <typename T>
VectorShard<T>::VectorShard(std::size_t capacity)
    : data_(0),
      capacity_(capacity),
      l_key_inferred_(SIZE_MAX),
      shard_(nullptr) {}

template <typename T>
VectorShard<T>::VectorShard(const VectorShard<T>::Shard *shard,
                            std::size_t capacity)
    : data_(0), capacity_(capacity), l_key_inferred_(SIZE_MAX), shard_(shard) {}

template <typename T>
VectorShard<T>::VectorShard(const std::vector<T> &data, std::size_t capacity)
    : data_(std::move(data)),
      capacity_(capacity),
      l_key_inferred_(SIZE_MAX),
      shard_(nullptr) {}

template <typename T>
VectorShard<T>::VectorShard(const VectorShard &o) {
  *this = o;
}

template <typename T>
VectorShard<T> &VectorShard<T>::operator=(const VectorShard &o) {
  data_ = o.data_;
  capacity_ = o.capacity_;
  l_key_inferred_ = o.l_key_inferred_;
  shard_ = o.shard_;
  return *this;
}

template <typename T>
VectorShard<T>::VectorShard(VectorShard &&o) noexcept
    : data_(o.data_),
      capacity_(o.capacity_),
      l_key_inferred_(o.l_key_inferred_),
      shard_(o.shard_) {}

template <typename T>
VectorShard<T>::VectorShard(const VectorShard<T>::Shard *shard,
                            VectorShard &&o) noexcept
    : data_(o.data_),
      capacity_(o.capacity_),
      l_key_inferred_(o.l_key_inferred_),
      shard_(shard) {}

template <typename T>
VectorShard<T> &VectorShard<T>::operator=(VectorShard &&o) noexcept {
  data_ = o.data_;
  capacity_ = o.capacity_;
  l_key_inferred_ = o.l_key_inferred_;
  shard_ = o.shard_;
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
  l_key_inferred_ = SIZE_MAX;
}

template <typename T>
void VectorShard<T>::emplace(Key k, Val v) {
  if (!shard_ && l_key_inferred_ == SIZE_MAX) {
    l_key_inferred_ = k;
  }

  auto l_key = this->l_key();
  auto r_key = this->r_key();

  if (k < l_key || k > r_key) {
    BUG_ON(k < l_key || k > r_key);
  }

  std::size_t idx = k - l_key;
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
  auto batch_l_key = shard.l_key();
  if (batch_l_key == SIZE_MAX) {
    data_.insert(data_.end(), shard.data_.begin(), shard.data_.end());
  } else {
    auto insert_loc = batch_l_key - l_key();
    assert(insert_loc <= data_.size());
    data_.resize(insert_loc + shard.data_.size());
    std::copy(shard.data_.begin(), shard.data_.end(),
              data_.begin() + insert_loc);
  }
}

template <typename T>
std::optional<T> VectorShard<T>::find(std::size_t k) {
  auto l_key = this->l_key();
  auto r_key = this->r_key();

  if (k < l_key || k > r_key) {
    return std::nullopt;
  }
  std::size_t idx = k - l_key;
  if (idx >= data_.size()) {
    return std::nullopt;
  }
  return std::optional{data_[idx]};
}

template <typename T>
std::pair<typename VectorShard<T>::Key, VectorShard<T>>
VectorShard<T>::split() {
  auto l_key = this->l_key();
  assert(l_key != SIZE_MAX);
  auto this_shard_size = std::min(data_.size(), capacity_);
  auto mid_key = l_key + this_shard_size;

  if (this_shard_size != data_.size()) {
    std::vector<T> remaining_elems(
        std::make_move_iterator(data_.begin() + this_shard_size),
        std::make_move_iterator(data_.end()));
    data_.resize(this_shard_size);
    return {mid_key, VectorShard<T>(remaining_elems, capacity_)};
  } else {
    VectorShard<T> new_shard;
    return {mid_key, new_shard};
  }
}

template <typename T>
template <typename... S0s, typename... S1s>
void VectorShard<T>::for_all(void (*fn)(std::pair<const Key, Val> &, S0s...),
                             S1s &&... states) {
  auto l_key = this->l_key();
  auto start_index = l_key == SIZE_MAX ? 0 : l_key;
  for (std::size_t i = 0; i < data_.size(); i++) {
    std::pair<const std::size_t, T> p = {start_index + i, data_[i]};
    fn(p, std::forward<S0s>(states)...);
    data_[i] = std::move(p.second);
  }
}

template <typename T>
std::size_t VectorShard<T>::l_key() const {
  if (shard_) {
    return shard_->l_key().value_or(SIZE_MAX);
  }
  return l_key_inferred_;
}

template <typename T>
std::size_t VectorShard<T>::r_key() const {
  if (shard_) {
    return shard_->r_key().value_or(SIZE_MAX);
  }
  return SIZE_MAX;
}

template <typename T>
template <class Archive>
void VectorShard<T>::save(Archive &ar) const {
  ar(data_);
  ar(capacity_);
  ar(l_key_inferred_);
  ar(shard_);
}

template <typename T>
template <class Archive>
void VectorShard<T>::load(Archive &ar) {
  ar(data_);
  ar(capacity_);
  ar(l_key_inferred_);
  ar(shard_);
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
void ShardedVector<T>::clear() {
  size_ = 0;
}

template <typename T>
ShardedVector<T>::ShardedVector(uint32_t max_shard_bytes,
                                uint32_t max_batch_bytes)
    : Base(max_shard_bytes, max_batch_bytes), size_(0) {}

template <typename T>
ShardedVector<T> make_sharded_vector(uint32_t max_shard_bytes,
                                     uint32_t max_batch_bytes) {
  return ShardedVector<T>(max_shard_bytes, max_batch_bytes);
}

}  // namespace nu
