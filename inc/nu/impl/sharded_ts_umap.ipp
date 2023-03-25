#pragma once

#include <algorithm>

namespace nu {

template <typename K, typename V, class H>
inline std::size_t GeneralTSUMap<K, V, H>::size() const {
  return 0;
}

template <typename K, typename V, class H>
inline bool GeneralTSUMap<K, V, H>::empty() const {
  return false;
}

template <typename K, typename V, class H>
inline std::size_t GeneralTSUMap<K, V, H>::insert(Key k, Val v) {
  map_.put(std::move(k), std::move(v));
  return 0;
}

template <typename K, typename V, class H>
inline std::optional<V> GeneralTSUMap<K, V, H>::find_data(Key k) const {
  return const_cast<GeneralTSUMap *>(this)->map_.get_copy(std::move(k));
}

template <typename K, typename V, class H>
inline bool GeneralTSUMap<K, V, H>::erase(Key k) {
  return map_.remove(std::move(k));
}

template <typename K, typename V, class H>
inline void GeneralTSUMap<K, V, H>::split(Key *mid_k,
                                          GeneralTSUMap *latter_half) {
  auto all_pairs = map_.get_all_pairs();
  sort(all_pairs.begin(), all_pairs.end());
  auto mid_iter = all_pairs.begin() + all_pairs.size() / 2;
  *mid_k = mid_iter->first;
  for (auto it = mid_iter; it != all_pairs.end(); ++it) {
    map_.remove(it->first);
    latter_half->map_.put(std::move(it->first), std::move(it->second));
  }
}

template <typename K, typename V, class H>
template <class Archive>
inline void GeneralTSUMap<K, V, H>::save(Archive &ar) const {
  ar(map_);
}

template <typename K, typename V, class H>
template <class Archive>
inline void GeneralTSUMap<K, V, H>::load(Archive &ar) {
  ar(map_);
}

template <typename K, typename V, typename H>
inline ShardedTSUMap<K, V, H>::ShardedTSUMap(
    std::optional<typename Base::ShardingHint> sharding_hint)
    : Base(sharding_hint, /* size_bound = */ std::nullopt) {}

template <typename K, typename V, typename H>
inline ShardedTSUMap<K, V, H> make_sharded_ts_umap() {
  return ShardedTSUMap<K, V, H>(std::nullopt);
}

}  // namespace nu
