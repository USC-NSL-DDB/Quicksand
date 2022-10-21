#include <memory>

#include "nu/utils/bfprt/median_of_ninthers.h"

namespace nu {

template <typename K, typename V>
inline PairCollectionConstIterator<K, V>::PairCollectionConstIterator() {}

template <typename K, typename V>
inline PairCollectionConstIterator<K, V>::PairCollectionConstIterator(
    std::span<const std::pair<K, V>>::iterator &&iter)
    : std::span<const std::pair<K, V>>::iterator(std::move(iter)) {}

template <typename K, typename V>
inline PairCollectionConstReverseIterator<
    K, V>::PairCollectionConstReverseIterator() {}

template <typename K, typename V>
inline PairCollectionConstReverseIterator<K, V>::
    PairCollectionConstReverseIterator(
        std::span<const std::pair<K, V>>::reverse_iterator &&iter)
    : std::span<const std::pair<K, V>>::reverse_iterator(std::move(iter)) {}

template <typename K, typename V>
inline PairCollection<K, V>::PairCollection()
    : data_(nullptr), size_(0), capacity_(0), ownership_(false) {}

template <typename K, typename V>
inline PairCollection<K, V>::PairCollection(const PairCollection &o)
    : data_(nullptr), capacity_(0) {
  size_ = o.size_;
  reserve(o.size_);
  std::copy(o.data_, o.data_ + size_, data_);
}

template <typename K, typename V>
inline PairCollection<K, V> &PairCollection<K, V>::operator=(
    const PairCollection &o) {
  size_ = o.size_;
  reserve(o.size_);
  std::copy(o.data_, o.data_ + size_, data_);
  return *this;
}

template <typename K, typename V>
inline PairCollection<K, V>::PairCollection(PairCollection &&o) noexcept
    : data_(o.data_),
      size_(o.size_),
      capacity_(o.capacity_),
      ownership_(o.ownership_) {
  o.data_ = nullptr;
}

template <typename K, typename V>
inline PairCollection<K, V> &PairCollection<K, V>::operator=(
    PairCollection &&o) noexcept {
  destroy();

  data_ = o.data_;
  size_ = o.size_;
  capacity_ = o.capacity_;
  ownership_ = o.ownership_;
  o.data_ = nullptr;
  return *this;
}

template <typename K, typename V>
inline void PairCollection<K, V>::destroy() {
  if (data_ && ownership_) {
    delete[] data_;
  }
}

template <typename K, typename V>
inline PairCollection<K, V>::~PairCollection() {
  destroy();
}

template <typename K, typename V>
inline std::size_t PairCollection<K, V>::size() const {
  return size_;
}

template <typename K, typename V>
inline std::size_t PairCollection<K, V>::capacity() const {
  return capacity_;
}

template <typename K, typename V>
inline bool PairCollection<K, V>::empty() const {
  return !size_;
}

template <typename K, typename V>
void PairCollection<K, V>::clear() {
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
void PairCollection<K, V>::reserve(std::size_t capacity) {
  if (unlikely(capacity <= capacity_)) {
    return;
  }

  capacity_ = capacity;
  ownership_ = true;
  auto *new_data = new std::pair<K, V>[capacity_];

  if (data_) {
    for (std::size_t i = 0; i < size_; i++) {
      new_data[i] = std::move(data_[i]);
    }
    delete[] data_;
  }

  data_ = new_data;
}

template <typename K, typename V>
inline void PairCollection<K, V>::emplace(K k, V v) {
  if (unlikely(size_ == capacity_)) {
    reserve(std::max(static_cast<std::size_t>(1), 2 * capacity_));
  }

  data_[size_++] = std::pair<K, V>(std::move(k), std::move(v));
  assert(size_ <= capacity_);
  assert(ownership_);
}

template <typename K, typename V>
void PairCollection<K, V>::merge(PairCollection pc) {
  if (unlikely(size_ + pc.size_ > capacity_)) {
    reserve(
        std::max(static_cast<std::size_t>(size_ + pc.size_), 2 * capacity_));
  }

  for (std::size_t i = 0; i < pc.size_; i++) {
    data_[size_++] = std::move(pc.data_[i]);
  }
  assert(size_ <= capacity_);
  assert(ownership_);
}

template <typename K, typename V>
void PairCollection<K, V>::split(K *mid_k, PairCollection<K, V> *latter_half) {
  adaptiveQuickselect(data_, size_ / 2, size_);
  *mid_k = data_[size_ / 2].first;
  latter_half->data_ = data_ + size_ / 2;
  latter_half->size_ = size_ - size_ / 2;
  latter_half->ownership_ = false;
  size_ /= 2;
}

template <typename K, typename V>
template <typename... S0s, typename... S1s>
inline void PairCollection<K, V>::for_all(void (*fn)(const K &key, V &val,
                                                     S0s...),
                                          S1s &&... states) {
  for (std::size_t i = 0; i < size_; i++) {
    fn(data_[i].first, data_[i].second, states...);
  }
}

template <typename K, typename V>
PairCollection<K, V>::ConstIterator PairCollection<K, V>::cbegin() const {
  return ConstIterator(std::span<const std::pair<K, V>>(data_, size_).begin());
}

template <typename K, typename V>
PairCollection<K, V>::ConstIterator PairCollection<K, V>::cend() const {
  return ConstIterator(std::span<const std::pair<K, V>>(data_, size_).end());
}

template <typename K, typename V>
PairCollection<K, V>::ConstReverseIterator PairCollection<K, V>::crbegin()
    const {
  return ConstReverseIterator(
      std::span<const std::pair<K, V>>(data_, size_).rbegin());
}

template <typename K, typename V>
PairCollection<K, V>::ConstReverseIterator PairCollection<K, V>::crend() const {
  return ConstReverseIterator(
      std::span<const std::pair<K, V>>(data_, size_).rend());
}

template <typename K, typename V>
template <class Archive>
void PairCollection<K, V>::save(Archive &ar) const {
  ar(size_);
  for (std::size_t i = 0; i < size_; i++) {
    ar(data_[i]);
  }
}

template <typename K, typename V>
template <class Archive>
void PairCollection<K, V>::load(Archive &ar) {
  ar(size_);
  reserve(size_);
  for (std::size_t i = 0; i < size_; i++) {
    ar(data_[i]);
  }
}

template <typename K, typename V>
inline std::pair<K, V> *PairCollection<K, V>::data() {
  return data_;
}

template <typename K, typename V>
inline ShardedPairCollection<K, V>::ShardedPairCollection(
    std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename K, typename V>
inline ShardedPairCollection<K, V> make_sharded_pair_collection() {
  return ShardedPairCollection<K, V>(std::nullopt);
}

template <typename K, typename V>
inline ShardedPairCollection<K, V> make_sharded_pair_collection(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn) {
  return ShardedPairCollection<K, V>(
      std::make_optional<typename ShardedDataStructure<
          PairCollectionContainer<K, V>, std::false_type>::Hint>(
          num, std::move(estimated_min_key), std::move(key_inc_fn)));
}

}  // namespace nu
