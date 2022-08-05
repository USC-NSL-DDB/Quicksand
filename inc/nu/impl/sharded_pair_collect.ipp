#include <memory>

#include "nu/utils/bfprt/median_of_ninthers.h"

namespace nu {

template <typename K, typename V>
PairCollection<K, V>::PairCollection()
    : data_(nullptr), size_(0), capacity_(0), ownership_(false) {}

template <typename K, typename V>
PairCollection<K, V>::PairCollection(std::size_t capacity) {
  data_ = new std::pair<K, V>[capacity];
  size_ = 0;
  capacity_ = capacity;
  ownership_ = true;
}

template <typename K, typename V>
PairCollection<K, V>::PairCollection(const Shard *shard, std::size_t capacity) {
  data_ = new std::pair<K, V>[capacity];
  size_ = 0;
  capacity_ = capacity;
  ownership_ = true;
}

template <typename K, typename V>
PairCollection<K, V>::PairCollection(const PairCollection &o)
    : data_(new std::pair<K, V>[o.capacity_]),
      size_(o.size_),
      capacity_(o.capacity_),
      ownership_(true) {
  std::copy(o.data_, o.data_ + size_, data_);
}

template <typename K, typename V>
PairCollection<K, V> &PairCollection<K, V>::operator=(const PairCollection &o) {
  destroy();

  size_ = o.size_;
  capacity_ = o.capacity_;
  data_ = new std::pair<K, V>[capacity_];
  std::copy(o.data_, o.data_ + size_, data_);
  ownership_ = true;
  return *this;
}

template <typename K, typename V>
PairCollection<K, V>::PairCollection(PairCollection &&o) noexcept
    : data_(o.data_),
      size_(o.size_),
      capacity_(o.capacity_),
      ownership_(o.ownership_) {
  o.data_ = nullptr;
}

template <typename K, typename V>
PairCollection<K, V>::PairCollection(const Shard *shard,
                                     PairCollection &&o) noexcept
    : data_(o.data_),
      size_(o.size_),
      capacity_(o.capacity_),
      ownership_(o.ownership_) {
  o.data_ = nullptr;
}

template <typename K, typename V>
PairCollection<K, V> &PairCollection<K, V>::operator=(
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
void PairCollection<K, V>::destroy() {
  if (data_ && ownership_) {
    delete[] data_;
  }
}

template <typename K, typename V>
PairCollection<K, V>::~PairCollection() {
  destroy();
}

template <typename K, typename V>
std::size_t PairCollection<K, V>::size() const {
  return size_;
}

template <typename K, typename V>
std::size_t PairCollection<K, V>::capacity() const {
  return capacity_;
}

template <typename K, typename V>
bool PairCollection<K, V>::empty() const {
  return !size_;
}

template <typename K, typename V>
void PairCollection<K, V>::clear() {
  if (data_) {
    for (std::size_t i = 0; i < size_; i++) {
      std::destroy_at(&data_[i]);
    }
  } else {
    // Moved. Reset it back into a specified sate.
    capacity_ = 0;
  }
  size_ = 0;
}

template <typename K, typename V>
void PairCollection<K, V>::expand() {
  if (!capacity_) {
    capacity_ = 1;
    ownership_ = true;
  } else {
    capacity_ *= 2;
  }
  auto *new_data = new std::pair<K, V>[capacity_];

  if (size_) {
    for (std::size_t i = 0; i < size_; i++) {
      new_data[i] = std::move(data_[i]);
    }
    delete[] data_;
  }

  data_ = new_data;
}

template <typename K, typename V>
void PairCollection<K, V>::emplace(K k, V v) {
  // Invoked at the client side for batching.
  if (unlikely(size_ == capacity_)) {
    expand();
  }
  data_[size_++] = std::pair<K, V>(std::move(k), std::move(v));
  assert(size_ <= capacity_);
  assert(ownership_);
}

template <typename K, typename V>
void PairCollection<K, V>::emplace_batch(PairCollection &&pc) {
  // Invoked at the Shard. Will never go out of bound due to the split design.
  for (std::size_t i = 0; i < pc.size_; i++) {
    data_[size_++] = std::move(pc.data_[i]);
  }
  assert(size_ <= capacity_);
  assert(ownership_);
}

template <typename K, typename V>
std::pair<K, PairCollection<K, V>> PairCollection<K, V>::split() {
  adaptiveQuickselect(data_, size_ / 2, size_);
  auto mid_k = data_[size_ / 2].first;
  PairCollection latter_half;
  latter_half.data_ = data_ + size_ / 2;
  latter_half.size_ = size_ - size_ / 2;
  latter_half.capacity_ = capacity_;
  latter_half.ownership_ = false;
  size_ /= 2;
  return std::make_pair(std::move(mid_k), std::move(latter_half));
}

template <typename K, typename V>
template <typename... S0s, typename... S1s>
void PairCollection<K, V>::for_all(void (*fn)(std::pair<const K, V> &, S0s...),
                                   S1s &&... states) {
  for (std::size_t i = 0; i < size_; i++) {
    fn(reinterpret_cast<std::pair<const K, V> &>(data_[i]), states...);
  }
}

template <typename K, typename V>
template <class Archive>
void PairCollection<K, V>::save(Archive &ar) const {
  ar(size_, capacity_);
  for (std::size_t i = 0; i < size_; i++) {
    ar(data_[i]);
  }
}

template <typename K, typename V>
template <class Archive>
void PairCollection<K, V>::load(Archive &ar) {
  ar(size_, capacity_);
  data_ = new std::pair<K, V>[capacity_];
  for (std::size_t i = 0; i < size_; i++) {
    ar(data_[i]);
  }
  ownership_ = true;
}

template <typename K, typename V>
std::pair<K, V> *PairCollection<K, V>::data() {
  return data_;
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(uint32_t max_shard_bytes,
                                                   uint32_t max_cache_bytes)
    : Base(max_shard_bytes, max_cache_bytes) {}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn, uint32_t max_shard_bytes,
    uint32_t max_cache_bytes)
    : Base(num, estimated_min_key, key_inc_fn, max_shard_bytes,
           max_cache_bytes) {}

template <typename K, typename V>
ShardedPairCollection<K, V> make_sharded_pair_collection(
    uint32_t max_shard_bytes, uint32_t max_cache_bytes) {
  return ShardedPairCollection<K, V>(max_shard_bytes, max_cache_bytes);
}

template <typename K, typename V>
ShardedPairCollection<K, V> make_sharded_pair_collection(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn, uint32_t max_shard_bytes,
    uint32_t max_cache_bytes) {
  return ShardedPairCollection<K, V>(num, std::move(estimated_min_key),
                                     std::move(key_inc_fn), max_shard_bytes,
                                     max_cache_bytes);
}

}  // namespace nu
