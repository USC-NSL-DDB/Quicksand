#include <algorithm>
#include <utility>

namespace nu {

template <class UMap>
inline UnorderedMapConstIterator<UMap>::UnorderedMapConstIterator() {}

template <class UMap>
inline UnorderedMapConstIterator<UMap>::UnorderedMapConstIterator(
    UMap::iterator &&iter) {
  UMap::const_iterator::operator=(std::move(iter));
}

template <class UMap>
inline UnorderedMapConstIterator<UMap>::UnorderedMapConstIterator(
    UMap::const_iterator &&iter) {
  UMap::const_iterator::operator=(std::move(iter));
}

template <typename K, typename V, typename H, BoolIntegral M>
inline GeneralUnorderedMap<K, V, H, M>::GeneralUnorderedMap(UMap initial_state)
    : map_(std::move(initial_state)) {}

template <typename K, typename V, typename H, BoolIntegral M>
inline std::size_t GeneralUnorderedMap<K, V, H, M>::size() const {
  return map_.size();
}

template <typename K, typename V, typename H, BoolIntegral M>
inline void GeneralUnorderedMap<K, V, H, M>::reserve(std::size_t size) {
  return map_.reserve(size);
}

template <typename K, typename V, typename H, BoolIntegral M>
inline bool GeneralUnorderedMap<K, V, H, M>::empty() const {
  return map_.empty();
}

template <typename K, typename V, typename H, BoolIntegral M>
inline void GeneralUnorderedMap<K, V, H, M>::clear() {
  map_.clear();
}

template <typename K, typename V, typename H, BoolIntegral M>
inline std::size_t GeneralUnorderedMap<K, V, H, M>::insert(Key k, Val v) {
  map_.emplace(std::move(k), std::move(v));
  return map_.size();
}

template <typename K, typename V, typename H, BoolIntegral M>
inline bool GeneralUnorderedMap<K, V, H, M>::erase(Key k) {
  return map_.erase(k);
}

template <typename K, typename V, typename H, BoolIntegral M>
inline void GeneralUnorderedMap<K, V, H, M>::merge(GeneralUnorderedMap m) {
  map_.merge(std::move(m.map_));
}

template <typename K, typename V, typename H, BoolIntegral M>
template <typename... S0s, typename... S1s>
inline void GeneralUnorderedMap<K, V, H, M>::for_all(
    void (*fn)(const Key &key, Val &val, S0s...), S1s &&...states) {
  for (auto &[k, v] : map_) {
    fn(k, v, states...);
  }
}

template <typename K, typename V, typename H, BoolIntegral M>
inline GeneralUnorderedMap<K, V, H, M>::ConstIterator
GeneralUnorderedMap<K, V, H, M>::find(K k) const {
  return map_.find(std::move(k));
}

template <typename K, typename V, typename H, BoolIntegral M>
inline std::optional<std::pair<K, V>>
GeneralUnorderedMap<K, V, H, M>::find_data(K k) const {
  auto iter = map_.find(std::move(k));
  return iter != map_.end() ? std::make_optional(*iter) : std::nullopt;
}

template <typename K, typename V, typename H, BoolIntegral M>
inline V &GeneralUnorderedMap<K, V, H, M>::operator[](K k) {
  return map_[k];
}

template <typename K, typename V, typename H, BoolIntegral M>
void GeneralUnorderedMap<K, V, H, M>::split(Key *mid_k,
                                            GeneralUnorderedMap *latter_half) {
  using Pair = std::pair<K, typename UMap::iterator>;

  std::vector<Pair> keys;
  keys.reserve(map_.size());
  for (auto iter = map_.begin(); iter != map_.end(); ++iter) {
    keys.emplace_back(iter->first, iter);
  }
  std::nth_element(
      keys.begin(), keys.begin() + keys.size() / 2, keys.end(),
      [](const Pair &x, const Pair &y) { return x.first < y.first; });
  *mid_k = keys[keys.size() / 2].first;

  for (std::size_t i = keys.size() / 2; i < keys.size(); i++) {
    auto node = map_.extract(keys[i].second);
    latter_half->map_.insert(std::move(node));
  }
}

template <typename K, typename V, typename H, BoolIntegral M>
inline GeneralUnorderedMap<K, V, H, M>::ConstIterator
GeneralUnorderedMap<K, V, H, M>::cbegin() const {
  return map_.cbegin();
}

template <typename K, typename V, typename H, BoolIntegral M>
inline GeneralUnorderedMap<K, V, H, M>::ConstIterator
GeneralUnorderedMap<K, V, H, M>::cend() const {
  return map_.cend();
}

template <typename K, typename V, typename H, BoolIntegral M>
template <class Archive>
inline void GeneralUnorderedMap<K, V, H, M>::save(Archive &ar) const {
  ar(map_);
}

template <typename K, typename V, typename H, BoolIntegral M>
template <class Archive>
inline void GeneralUnorderedMap<K, V, H, M>::load(Archive &ar) {
  ar(map_);
}

template <typename K, typename V, typename H, typename M, typename LL>
inline GeneralShardedUnorderedMap<K, V, H, M, LL>::GeneralShardedUnorderedMap(
    std::optional<typename Base::ShardingHint> sharding_hint)
    : Base(sharding_hint,
           /* pinned_ip = */ std::nullopt) {}

template <typename K, typename V, typename H, typename M, typename LL>
template <typename RetT, typename... S0s, typename... S1s>
inline RetT GeneralShardedUnorderedMap<K, V, H, M, LL>::apply_on(
    K k, RetT (*fn)(V *v, S0s...), S1s &&...states) {
  using Fn = decltype(fn);
  auto fn_addr = reinterpret_cast<uintptr_t>(fn);
  return this->run(
      k,
      +[](GeneralUnorderedMap<K, V, H, M> &map, K k, uintptr_t fn_addr,
          S0s... states) {
        auto *fn = reinterpret_cast<Fn>(fn_addr);
        auto iter = map.find(k);
        auto *ptr = const_cast<V *>(&iter->second);
        return fn(ptr, states...);
      },
      fn_addr, std::forward<S1s>(states)...);
}

template <typename K, typename V, typename H, typename M, typename LL>
template <typename RetT, typename... S0s, typename... S1s>
inline RetT GeneralShardedUnorderedMap<K, V, H, M, LL>::apply_on(
    K k, RetT (*fn)(V &v, S0s...), S1s &&...states) {
  using Fn = decltype(fn);
  auto fn_addr = reinterpret_cast<uintptr_t>(fn);
  return this->run(
      k,
      +[](GeneralUnorderedMap<K, V, H, M> &map, K k, uintptr_t fn_addr,
          S0s... states) {
        auto *fn = reinterpret_cast<Fn>(fn_addr);
        return fn(map[k], states...);
      },
      fn_addr, std::forward<S1s>(states)...);
}

template <typename K, typename V, typename H, typename LL>
inline ShardedUnorderedMap<K, V, H, LL> make_sharded_unordered_map() {
  return ShardedUnorderedMap<K, V, H, LL>(std::nullopt);
}

template <typename K, typename V, typename H, typename LL>
inline ShardedUnorderedMultiMap<K, V, H, LL> make_sharded_unordered_multimap() {
  return ShardedUnorderedMultiMap<K, V, H, LL>(std::nullopt);
}

}  // namespace nu
