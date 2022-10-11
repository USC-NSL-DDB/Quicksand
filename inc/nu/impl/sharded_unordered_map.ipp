#include <algorithm>
#include <utility>

namespace nu {

template <class USet>
UnorderedMapConstIterator<USet>::UnorderedMapConstIterator() {}

template <class USet>
UnorderedMapConstIterator<USet>::UnorderedMapConstIterator(
    USet::iterator &&iter) {
  USet::const_iterator::operator=(std::move(iter));
}

template <class USet>
UnorderedMapConstIterator<USet>::UnorderedMapConstIterator(
    USet::const_iterator &&iter) {
  USet::const_iterator::operator=(std::move(iter));
}

template <typename K, typename V, typename M>
GeneralUnorderedMap<K, V, M>::GeneralUnorderedMap(std::optional<Key> l_key) {}

template <typename K, typename V, typename M>
GeneralUnorderedMap<K, V, M>::GeneralUnorderedMap(std::optional<Key> l_key,
                                                  std::size_t capacity) {
  map_.reserve(capacity);
}

template <typename K, typename V, typename M>
GeneralUnorderedMap<K, V, M>::GeneralUnorderedMap(UMap initial_state)
    : map_(std::move(initial_state)) {}

template <typename K, typename V, typename M>
std::size_t GeneralUnorderedMap<K, V, M>::size() const {
  return map_.size();
}

template <typename K, typename V, typename M>
void GeneralUnorderedMap<K, V, M>::reserve(std::size_t size) {
  return map_.reserve(size);
}

template <typename K, typename V, typename M>
bool GeneralUnorderedMap<K, V, M>::empty() const {
  return map_.empty();
}

template <typename K, typename V, typename M>
void GeneralUnorderedMap<K, V, M>::clear() {
  map_.clear();
}

template <typename K, typename V, typename M>
void GeneralUnorderedMap<K, V, M>::emplace(Key k, Val v) {
  map_.emplace(std::move(k), std::move(v));
}

template <typename K, typename V, typename M>
void GeneralUnorderedMap<K, V, M>::merge(GeneralUnorderedMap m) {
  for (auto &[k, v] : m.map_) {
    map_[std::move(k)] = std::move(v);
  }
}

template <typename K, typename V, typename M>
template <typename... S0s, typename... S1s>
void GeneralUnorderedMap<K, V, M>::for_all(void (*fn)(const Key &key, Val &val,
                                                      S0s...),
                                           S1s &&... states) {
  for (auto &[k, v] : map_) {
    fn(k, v, states...);
  }
}

template <typename K, typename V, typename M>
GeneralUnorderedMap<K, V, M>::ConstIterator GeneralUnorderedMap<K, V, M>::find(
    K k) {
  return map_.find(std::move(k));
}

template <typename K, typename V, typename M>
std::pair<K, GeneralUnorderedMap<K, V, M>>
GeneralUnorderedMap<K, V, M>::split() {
  assert(!map_.empty());

  std::vector<K> keys;
  keys.reserve(map_.size());
  for (const auto &[k, v] : map_) {
    keys.push_back(k);
  }

  std::nth_element(keys.begin(), keys.begin() + keys.size() / 2, keys.end());
  auto mid_key = keys[keys.size() / 2];

  UMap latter_half_map;
  for (auto it = map_.cbegin(); it != map_.cend();) {
    if (it->first >= mid_key) {
      latter_half_map.emplace(std::move(it->first), std::move(it->second));
      map_.erase(it++);
    } else {
      ++it;
    }
  }

  GeneralUnorderedMap latter_half_container(std::move(latter_half_map));

  return std::make_pair(mid_key, std::move(latter_half_container));
}

template <typename K, typename V, typename M>
GeneralUnorderedMap<K, V, M>::ConstIterator
GeneralUnorderedMap<K, V, M>::cbegin() const {
  return map_.cbegin();
}

template <typename K, typename V, typename M>
GeneralUnorderedMap<K, V, M>::ConstIterator GeneralUnorderedMap<K, V, M>::cend()
    const {
  return map_.cend();
}

template <typename K, typename V, typename M>
template <class Archive>
void GeneralUnorderedMap<K, V, M>::save(Archive &ar) const {
  ar(map_);
}

template <typename K, typename V, typename M>
template <class Archive>
void GeneralUnorderedMap<K, V, M>::load(Archive &ar) {
  ar(map_);
}

template <typename K, typename V, typename M, typename LL>
V GeneralShardedUnorderedMap<K, V, M, LL>::operator[](const K &key) {
  auto found = this->find_val(key);
  if (found.has_value()) {
    return found->second;
  } else {
    auto default_val = V();
    this->emplace(key, default_val);
    return default_val;
  }
}

template <typename K, typename V, typename M, typename LL>
GeneralShardedUnorderedMap<K, V, M, LL>::GeneralShardedUnorderedMap(
    std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename K, typename V, typename LL>
ShardedUnorderedMap<K, V, LL> make_sharded_unordered_map() {
  return ShardedUnorderedMap<K, V, LL>(std::nullopt);
}

template <typename K, typename V, typename LL>
ShardedUnorderedMultiMap<K, V, LL> make_sharded_unordered_multimap() {
  return ShardedUnorderedMultiMap<K, V, LL>(std::nullopt);
}

}  // namespace nu
