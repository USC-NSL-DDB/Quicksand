#include "nu/utils/bfprt/median_of_ninthers.h"

namespace nu {

template <typename K, typename V>
std::size_t PairCollection<K, V>::size() const {
  return data_.size();
}

template <typename K, typename V>
bool PairCollection<K, V>::empty() const {
  return data_.empty();
}

template <typename K, typename V>
void PairCollection<K, V>::clear() {
  data_.clear();
}

template <typename K, typename V>
void PairCollection<K, V>::reserve(std::size_t size) {
  data_.reserve(size);
}

template <typename K, typename V>
void PairCollection<K, V>::emplace(K k, V v) {
  data_.emplace_back(std::move(k), std::move(v));
}

template <typename K, typename V>
void PairCollection<K, V>::emplace_batch(
    GeneralContainer<K, V> &general_container) {
  auto &container = dynamic_cast<PairCollection &>(general_container);
  data_.insert(data_.end(), container.data_.begin(), container.data_.end());
}

template <typename K, typename V>
void PairCollection<K, V>::split(
    K *mid_k_ptr, GeneralContainer<K, V> *general_latter_half_ptr) {
  auto *latter_half_ptr = dynamic_cast<PairCollection *>(general_latter_half_ptr);
  adaptiveQuickselect(data_.data(), data_.size() / 2, data_.size());
  *mid_k_ptr = data_[data_.size() / 2].first;
  auto mid = data_.begin() + data_.size() / 2;
  latter_half_ptr->data_ = std::vector(mid, data_.end());
  data_.erase(mid, data_.end());
}

template <typename K, typename V>
void PairCollection<K, V>::merge(GeneralContainer<K, V> &general_container) {
  auto &container = dynamic_cast<PairCollection &>(general_container);
  data_.insert(data_.end(), std::make_move_iterator(container.data_.begin()),
               std::make_move_iterator(container.data_.end()));
}

template <typename K, typename V>
void PairCollection<K, V>::for_all(
    std::function<void(std::pair<const K, V> &)> fn) {
  for (auto &p : data_) {
    fn(reinterpret_cast<std::pair<const K, V> &>(p));
  }
}

template <typename K, typename V>
void PairCollection<K, V>::save(cereal::BinaryOutputArchive &ar) const {
  ar(data_);
}

template <typename K, typename V>
void PairCollection<K, V>::load(cereal::BinaryInputArchive &ar) {
  ar(data_);
}

template <typename K, typename V>
std::vector<std::pair<K, V>> &PairCollection<K, V>::unwrap() {
  return data_;
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(
    std::optional<K> initial_l_key, std::optional<K> initial_r_key,
    uint32_t max_shard_bytes, uint32_t max_cache_bytes)
    : Base(initial_l_key, initial_r_key, max_shard_bytes,
                                 max_cache_bytes) {}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn, uint32_t max_shard_bytes,
    uint32_t max_cache_bytes)
    : Base(num, estimated_min_key, key_inc_fn,
                                 max_shard_bytes, max_cache_bytes) {}

template <typename K, typename V>
ShardedPairCollection<K, V> make_sharded_pair_collection(
    uint32_t max_shard_bytes, uint32_t max_cache_bytes) {
  return ShardedPairCollection<K, V>(std::optional<K>(), std::optional<K>(),
                                     max_shard_bytes, max_cache_bytes);
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
