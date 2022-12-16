#include <algorithm>
#include <memory>

#include "nu/cereal.hpp"
#include "nu/utils/bfprt/median_of_ninthers.h"

namespace nu {

template <typename K, typename V>
inline PartitionerConstIterator<K, V>::PartitionerConstIterator() {}

template <typename K, typename V>
inline PartitionerConstIterator<K, V>::PartitionerConstIterator(
    std::span<const typename Partitioner<K, V>::DataEntry>::iterator &&iter)
    : std::span<const typename Partitioner<K, V>::DataEntry>::iterator(
          std::move(iter)) {}

template <typename K, typename V>
inline PartitionerConstReverseIterator<K,
                                       V>::PartitionerConstReverseIterator() {}

template <typename K, typename V>
inline PartitionerConstReverseIterator<K, V>::PartitionerConstReverseIterator(
    std::span<const typename Partitioner<K, V>::DataEntry>::reverse_iterator
        &&iter)
    : std::span<const typename Partitioner<K, V>::DataEntry>::reverse_iterator(
          std::move(iter)) {}

template <typename K, typename V>
inline Partitioner<K, V>::Partitioner()
    : data_(nullptr), size_(0), capacity_(0), ownership_(false) {}

template <typename K, typename V>
inline Partitioner<K, V>::Partitioner(const Partitioner &o)
    : data_(nullptr), capacity_(0) {
  size_ = o.size_;
  reserve(o.size_);
  std::copy(o.data_, o.data_ + size_, data_);
}

template <typename K, typename V>
inline Partitioner<K, V> &Partitioner<K, V>::operator=(const Partitioner &o) {
  size_ = o.size_;
  reserve(o.size_);
  std::copy(o.data_, o.data_ + size_, data_);
  return *this;
}

template <typename K, typename V>
inline Partitioner<K, V>::Partitioner(Partitioner &&o) noexcept
    : data_(o.data_),
      size_(o.size_),
      capacity_(o.capacity_),
      ownership_(o.ownership_) {
  o.data_ = nullptr;
}

template <typename K, typename V>
inline Partitioner<K, V> &Partitioner<K, V>::operator=(
    Partitioner &&o) noexcept {
  destroy();

  data_ = o.data_;
  size_ = o.size_;
  capacity_ = o.capacity_;
  ownership_ = o.ownership_;
  o.data_ = nullptr;
  return *this;
}

template <typename K, typename V>
inline void Partitioner<K, V>::destroy() {
  if (data_ && ownership_) {
    delete[] data_;
  }
}

template <typename K, typename V>
inline Partitioner<K, V>::~Partitioner() {
  destroy();
}

template <typename K, typename V>
inline std::size_t Partitioner<K, V>::size() const {
  return size_;
}

template <typename K, typename V>
inline std::size_t Partitioner<K, V>::capacity() const {
  return capacity_;
}

template <typename K, typename V>
inline bool Partitioner<K, V>::empty() const {
  return !size_;
}

template <typename K, typename V>
void Partitioner<K, V>::clear() {
  if (data_) {
    for (std::size_t i = 0; i < size_; i++) {
      std::destroy_at(&data_[i]);
    }
  } else {
    // Moved. Reset it back into a clean state.
    capacity_ = 0;
  }
  size_ = 0;
}

template <typename K, typename V>
void Partitioner<K, V>::reserve(std::size_t capacity) {
  if (unlikely(capacity <= capacity_)) {
    return;
  }

  capacity_ = capacity;
  ownership_ = true;
  auto *new_data = new DataEntry[capacity_];

  if (data_) {
    for (std::size_t i = 0; i < size_; i++) {
      new_data[i] = std::move(data_[i]);
    }
    delete[] data_;
  }

  data_ = new_data;
}

template <typename K, typename V>
inline std::size_t Partitioner<K, V>::emplace(
    K k, V v) requires HasVal<Partitioner> {
  if (unlikely(size_ == capacity_)) {
    reserve(std::max(static_cast<std::size_t>(1), 2 * capacity_));
  }

  data_[size_++] = std::make_pair(std::move(k), std::move(v));
  assert(size_ <= capacity_);
  assert(ownership_);

  return size_;
}

template <typename K, typename V>
inline std::size_t Partitioner<K, V>::emplace(K k) requires(
    !HasVal<Partitioner>) {
  if (unlikely(size_ == capacity_)) {
    reserve(std::max(static_cast<std::size_t>(1), 2 * capacity_));
  }

  data_[size_++] = std::move(k);
  assert(size_ <= capacity_);
  assert(ownership_);

  return size_;
}

template <typename K, typename V>
void Partitioner<K, V>::merge(Partitioner partitioner) {
  if (unlikely(size_ + partitioner.size_ > capacity_)) {
    reserve(std::max(static_cast<std::size_t>(size_ + partitioner.size_),
                     2 * capacity_));
  }

  for (std::size_t i = 0; i < partitioner.size_; i++) {
    data_[size_++] = std::move(partitioner.data_[i]);
  }
  assert(size_ <= capacity_);
  assert(ownership_);
}

template <typename K, typename V>
void Partitioner<K, V>::split(K *mid_k, Partitioner<K, V> *latter_half) {
  adaptiveQuickselect(data_, size_ / 2, size_);
  if constexpr (HasVal<Partitioner>) {
    *mid_k = data_[size_ / 2].first;
  } else {
    *mid_k = data_[size_ / 2];
  }
  latter_half->data_ = data_ + size_ / 2;
  latter_half->size_ = size_ - size_ / 2;
  latter_half->ownership_ = false;
  size_ /= 2;
}

template <typename K, typename V>
template <typename... S0s, typename... S1s>
inline void Partitioner<K, V>::for_all(
    void (*fn)(const K &key, V &val, S0s...),
    S1s &&... states) requires HasVal<Partitioner> {
  for (std::size_t i = 0; i < size_; i++) {
    fn(data_[i].first, data_[i].second, states...);
  }
}

template <typename K, typename V>
template <typename... S0s, typename... S1s>
inline void Partitioner<K, V>::for_all(
    void (*fn)(const K &key, S0s...),
    S1s &&... states) requires(!HasVal<Partitioner>) {
  for (std::size_t i = 0; i < size_; i++) {
    fn(data_[i], states...);
  }
}

template <typename K, typename V>
inline Partitioner<K, V>::ConstIterator Partitioner<K, V>::find_by_order(
    std::size_t order) {
  if (unlikely(order >= size_)) {
    return cend();
  }
  adaptiveQuickselect(data_, order, size_);
  return cbegin() + order;
}

template <typename K, typename V>
Partitioner<K, V>::ConstIterator Partitioner<K, V>::cbegin() const {
  return ConstIterator(std::span<const DataEntry>(data_, size_).begin());
}

template <typename K, typename V>
Partitioner<K, V>::ConstIterator Partitioner<K, V>::cend() const {
  return ConstIterator(std::span<const DataEntry>(data_, size_).end());
}

template <typename K, typename V>
Partitioner<K, V>::ConstReverseIterator Partitioner<K, V>::crbegin() const {
  return ConstReverseIterator(
      std::span<const DataEntry>(data_, size_).rbegin());
}

template <typename K, typename V>
Partitioner<K, V>::ConstReverseIterator Partitioner<K, V>::crend() const {
  return ConstReverseIterator(std::span<const DataEntry>(data_, size_).rend());
}

template <typename K, typename V>
void Partitioner<K, V>::sort() {
  std::sort(data_, data_ + size_);
}

template <typename K, typename V>
template <class Archive>
void Partitioner<K, V>::save(Archive &ar) const {
  ar(size_);

  if constexpr (cereal::is_memcpy_safe<decltype(data_[0])>()) {
    ar(cereal::binary_data(data_, size_ * sizeof(data_[0])));
  } else {
    for (std::size_t i = 0; i < size_; i++) {
      ar(data_[i]);
    }
  }
}

template <typename K, typename V>
template <class Archive>
void Partitioner<K, V>::load(Archive &ar) {
  ar(size_);
  reserve(size_);

  if constexpr (cereal::is_memcpy_safe<decltype(data_[0])>()) {
    ar(cereal::binary_data(data_, size_ * sizeof(data_[0])));
  } else {
    for (std::size_t i = 0; i < size_; i++) {
      ar(data_[i]);
    }
  }
}

template <typename K, typename V>
inline Partitioner<K, V>::DataEntry *Partitioner<K, V>::data() {
  return data_;
}

template <typename K, typename V>
inline ShardedPartitioner<K, V>::ShardedPartitioner(
    std::optional<typename Base::Hint> hint)
    : Base(hint, /* size_bound = */ std::nullopt) {}

template <typename K, typename V>
inline ShardedPartitioner<K, V> make_sharded_partitioner() {
  return ShardedPartitioner<K, V>(std::nullopt);
}

template <typename K, typename V>
inline ShardedPartitioner<K, V> make_sharded_partitioner(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn) {
  return ShardedPartitioner<K, V>(
      std::make_optional<typename ShardedDataStructure<
          PartitionerContainer<K, V>, std::false_type>::Hint>(
          num, std::move(estimated_min_key), std::move(key_inc_fn)));
}

}  // namespace nu
