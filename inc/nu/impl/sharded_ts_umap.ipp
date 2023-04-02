#pragma once

#include <algorithm>
#include <limits>

namespace nu {

template <typename K, typename V, class H>
inline std::size_t GeneralTSUMap<K, V, H>::size() const {
  return 1;
}

template <typename K, typename V, class H>
inline bool GeneralTSUMap<K, V, H>::empty() const {
  return false;
}

template <typename K, typename V, class H>
inline std::size_t GeneralTSUMap<K, V, H>::insert(std::size_t hash, K k, V v) {
  map_.put_with_hash(std::move(k), std::move(v), hash);
  return 1;
}

template <typename K, typename V, class H>
inline std::optional<V> GeneralTSUMap<K, V, H>::find_data(std::size_t hash,
                                                          K k) const {
  return const_cast<GeneralTSUMap *>(this)->map_.get_copy_with_hash(
      std::move(k), hash);
}

template <typename K, typename V, class H>
inline bool GeneralTSUMap<K, V, H>::erase(std::size_t hash, K k) {
  return map_.remove_with_hash(std::move(k), hash);
}

template <typename K, typename V, class H>
inline void GeneralTSUMap<K, V, H>::split(Key *mid_k,
                                          GeneralTSUMap *latter_half) {
  auto hashes_and_keys = map_.get_all_hashes_and_keys();
  std::sort(hashes_and_keys.begin(), hashes_and_keys.end());
  auto mid_iter = hashes_and_keys.begin() + hashes_and_keys.size() / 2;
  *mid_k = mid_iter->first;
  for (auto it = mid_iter; it != hashes_and_keys.end(); it++) {
    auto v = map_.get_and_remove(it->second);
    assert(v);
    latter_half->map_.put_with_hash(std::move(it->second), std::move(*v),
                                    it->first);
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
template <typename RetT, typename... S0s, typename... S1s>
inline RetT ShardedTSUMap<K, V, H>::apply_on(
    K k, RetT (*fn)(std::pair<const K, V> &, S0s...), S1s &&...states) {
  using Fn = decltype(fn);
  auto fn_addr = reinterpret_cast<uintptr_t>(fn);
  auto hash = hasher_(k);
  return this->run(
      hash,
      +[](GeneralTSUMap<K, V, H> &map, std::size_t hash, uintptr_t fn_addr, K k,
          S0s... states) {
        auto *fn = reinterpret_cast<Fn>(fn_addr);
        return map.map_.apply_with_hash(std::move(k), hash, fn, states...);
      },
      fn_addr, std::move(k), std::forward<S1s>(states)...);
}

template <typename K, typename V, typename H>
inline std::optional<V> ShardedTSUMap<K, V, H>::find_data(K k) const {
  auto hash = hasher_(k);
  return const_cast<ShardedTSUMap *>(this)->run(
      hash,
      +[](GeneralTSUMap<K, V, H> &map, std::size_t hash, K k) {
        return map.find_data(hash, std::move(k));
      },
      std::move(k));
}

template <typename K, typename V, typename H>
inline bool ShardedTSUMap<K, V, H>::erase(const K &k) {
  auto hash = hasher_(k);
  return this->run(
      hash,
      +[](GeneralTSUMap<K, V, H> &map, std::size_t hash, K k) {
        return map.erase(hash, std::move(k));
      },
      std::move(k));
}

template <typename K, typename V, typename H>
template <typename K1, typename V1>
inline void ShardedTSUMap<K, V, H>::insert(K1 &&k, V1 &&v) {
  auto hash = hasher_(k);
  this->run(
      hash,
      +[](GeneralTSUMap<K, V, H> &map, std::size_t hash, K k, V v) {
        return map.insert(hash, std::move(k), std::move(v));
      },
      std::move(k), std::move(v));
}

template <typename K, typename V, typename H>
inline ShardedTSUMap<K, V, H> make_sharded_ts_umap() {
  using Base = ShardedDataStructure<GeneralContainer<GeneralTSUMap<K, V, H>>,
                                    std::true_type>;
  typename Base::ShardingHint h;
  h.num = ShardedTSUMap<K, V, H>::kNumInitialShards *
          (Base::kLowLatencyMaxShardBytes / sizeof(typename Base::DataEntry));
  auto delta = std::numeric_limits<uint64_t>::max() /
               ShardedTSUMap<K, V, H>::kNumInitialShards;
  h.estimated_min_key = 0;
  h.key_inc_fn =
      std::function([delta](uint64_t &k, uint64_t _) { k += delta; });
  return ShardedTSUMap<K, V, H>(h);
}

}  // namespace nu
