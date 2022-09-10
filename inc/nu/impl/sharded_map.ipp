#include <cereal/types/map.hpp>

namespace nu {

template <class Map>
MapConstIterator<Map>::MapConstIterator() {}

template <class Map>
MapConstIterator<Map>::MapConstIterator(Map::iterator &&iter) {
  Map::const_iterator::operator=(std::move(iter));
}

template <class Map>
MapConstIterator<Map>::MapConstIterator(Map::const_iterator &&iter) {
  Map::const_iterator::operator=(std::move(iter));
}

template <class Map>
template <class Archive>
void MapConstIterator<Map>::serialize(Archive &ar) {
  ar(cereal::binary_data(this, sizeof(*this)));
}

template <class Map>
MapConstReverseIterator<Map>::MapConstReverseIterator() {}

template <class Map>
MapConstReverseIterator<Map>::MapConstReverseIterator(
    Map::reverse_iterator &&iter) {
  Map::const_reverse_iterator::operator=(std::move(iter));
}

template <class Map>
MapConstReverseIterator<Map>::MapConstReverseIterator(
    Map::const_reverse_iterator &&iter) {
  Map::const_reverse_iterator::operator=(std::move(iter));
}

template <class Map>
template <class Archive>
void MapConstReverseIterator<Map>::serialize(Archive &ar) {
  ar(cereal::binary_data(this, sizeof(*this)));
}

template <typename K, typename V, typename M>
GeneralMap<K, V, M>::GeneralMap(std::size_t capacity) {}

template <typename K, typename V, typename M>
GeneralMap<K, V, M>::GeneralMap(Map initial_state)
    : map_(std::move(initial_state)) {}

template <typename K, typename V, typename M>
std::size_t GeneralMap<K, V, M>::size() const {
  return map_.size();
}

template <typename K, typename V, typename M>
bool GeneralMap<K, V, M>::empty() const {
  return map_.empty();
}

template <typename K, typename V, typename M>
void GeneralMap<K, V, M>::clear() {
  map_.clear();
}

template <typename K, typename V, typename M>
void GeneralMap<K, V, M>::emplace(Key k, Val v) {
  map_.emplace(std::move(k), std::move(v));
}

template <typename K, typename V, typename M>
void GeneralMap<K, V, M>::merge(GeneralMap m) {
  for (auto &[k, v] : m.map_) {
    map_[std::move(k)] = std::move(v);
  }
}

template <typename K, typename V, typename M>
template <typename... S0s, typename... S1s>
void GeneralMap<K, V, M>::for_all(void (*fn)(const Key &key, Val &val, S0s...),
                                  S1s &&... states) {
  for (auto &[k, v] : map_) {
    fn(k, v, states...);
  }
}

template <typename K, typename V, typename M>
GeneralMap<K, V, M>::ConstIterator GeneralMap<K, V, M>::find(K k) {
  return map_.find(std::move(k));
}

template <typename K, typename V, typename M>
std::pair<K, GeneralMap<K, V, M>> GeneralMap<K, V, M>::split() {
  assert(!map_.empty());

  auto mid = map_.size() / 2;
  auto split_it = map_.begin();
  std::advance(split_it, mid);

  auto latter_half_l_key = split_it->first;

  Map latter_half_map(std::make_move_iterator(split_it),
                      std::make_move_iterator(map_.end()));
  map_.erase(split_it, map_.end());

  GeneralMap latter_half_container(std::move(latter_half_map));
  return std::make_pair(latter_half_l_key, std::move(latter_half_container));
}

template <typename K, typename V, typename M>
GeneralMap<K, V, M>::ConstIterator GeneralMap<K, V, M>::cbegin() const {
  return map_.cbegin();
}

template <typename K, typename V, typename M>
GeneralMap<K, V, M>::ConstIterator GeneralMap<K, V, M>::cend() const {
  return map_.cend();
}

template <typename K, typename V, typename M>
GeneralMap<K, V, M>::ConstReverseIterator GeneralMap<K, V, M>::crbegin() const {
  return map_.crbegin();
}

template <typename K, typename V, typename M>
GeneralMap<K, V, M>::ConstReverseIterator GeneralMap<K, V, M>::crend() const {
  return map_.crend();
}

template <typename K, typename V, typename M>
template <class Archive>
void GeneralMap<K, V, M>::save(Archive &ar) const {
  ar(map_);
}

template <typename K, typename V, typename M>
template <class Archive>
void GeneralMap<K, V, M>::load(Archive &ar) {
  ar(map_);
}

template <typename K, typename V, typename M, typename LL>
V GeneralShardedMap<K, V, M, LL>::operator[](const K &key) {
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
GeneralShardedMap<K, V, M, LL>::GeneralShardedMap(
    std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename K, typename V, typename LL>
ShardedMap<K, V, LL> make_sharded_map() {
  return ShardedMap<K, V, LL>(std::nullopt);
}

template <typename K, typename V, typename LL>
ShardedMultiMap<K, V, LL> make_sharded_multi_map() {
  return ShardedMultiMap<K, V, LL>(std::nullopt);
}

}  // namespace nu
