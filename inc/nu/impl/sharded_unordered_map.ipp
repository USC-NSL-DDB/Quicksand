#include <algorithm>
#include <utility>

namespace nu {

template <class USet>
inline UnorderedMapConstIterator<USet>::UnorderedMapConstIterator() {}

template <class USet>
inline UnorderedMapConstIterator<USet>::UnorderedMapConstIterator(
    USet::iterator &&iter) {
  USet::const_iterator::operator=(std::move(iter));
}

template <class USet>
inline UnorderedMapConstIterator<USet>::UnorderedMapConstIterator(
    USet::const_iterator &&iter) {
  USet::const_iterator::operator=(std::move(iter));
}

template <typename K, typename V, typename M>
inline GeneralUnorderedMap<K, V, M>::GeneralUnorderedMap(UMap initial_state)
    : map_(std::move(initial_state)) {}

template <typename K, typename V, typename M>
inline std::size_t GeneralUnorderedMap<K, V, M>::size() const {
  return map_.size();
}

template <typename K, typename V, typename M>
inline void GeneralUnorderedMap<K, V, M>::reserve(std::size_t size) {
  return map_.reserve(size);
}

template <typename K, typename V, typename M>
inline bool GeneralUnorderedMap<K, V, M>::empty() const {
  return map_.empty();
}

template <typename K, typename V, typename M>
inline void GeneralUnorderedMap<K, V, M>::clear() {
  map_.clear();
}

template <typename K, typename V, typename M>
inline std::size_t GeneralUnorderedMap<K, V, M>::insert(Key k, Val v) {
  map_.emplace(std::move(k), std::move(v));
  return map_.size();
}

template <typename K, typename V, typename M>
inline void GeneralUnorderedMap<K, V, M>::merge(GeneralUnorderedMap m) {
  map_.merge(std::move(m.map_));
}

template <typename K, typename V, typename M>
template <typename... S0s, typename... S1s>
inline void GeneralUnorderedMap<K, V, M>::for_all(void (*fn)(const Key &key,
                                                             Val &val, S0s...),
                                                  S1s &&... states) {
  for (auto &[k, v] : map_) {
    fn(k, v, states...);
  }
}

template <typename K, typename V, typename M>
inline GeneralUnorderedMap<K, V, M>::ConstIterator
GeneralUnorderedMap<K, V, M>::find(K k) const {
  return map_.find(std::move(k));
}

template <typename K, typename V, typename M>
void GeneralUnorderedMap<K, V, M>::split(Key *mid_k,
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

template <typename K, typename V, typename M>
inline GeneralUnorderedMap<K, V, M>::ConstIterator
GeneralUnorderedMap<K, V, M>::cbegin() const {
  return map_.cbegin();
}

template <typename K, typename V, typename M>
inline GeneralUnorderedMap<K, V, M>::ConstIterator
GeneralUnorderedMap<K, V, M>::cend() const {
  return map_.cend();
}

template <typename K, typename V, typename M>
template <class Archive>
inline void GeneralUnorderedMap<K, V, M>::save(Archive &ar) const {
  ar(map_);
}

template <typename K, typename V, typename M>
template <class Archive>
inline void GeneralUnorderedMap<K, V, M>::load(Archive &ar) {
  ar(map_);
}

template <typename K, typename V, typename M, typename LL>
inline GeneralShardedUnorderedMap<K, V, M, LL>::GeneralShardedUnorderedMap(
    std::optional<typename Base::Hint> hint)
    : Base(hint, /* size_bound = */ std::nullopt) {}

template <typename K, typename V, typename LL>
inline ShardedUnorderedMap<K, V, LL> make_sharded_unordered_map() {
  return ShardedUnorderedMap<K, V, LL>(std::nullopt);
}

template <typename K, typename V, typename LL>
inline ShardedUnorderedMultiMap<K, V, LL> make_sharded_unordered_multimap() {
  return ShardedUnorderedMultiMap<K, V, LL>(std::nullopt);
}

}  // namespace nu
