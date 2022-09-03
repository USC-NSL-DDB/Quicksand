#include <cereal/types/map.hpp>

namespace nu {
template <typename K, typename V>
MapConstIterator<K, V>::MapConstIterator() {}

template <typename K, typename V>
MapConstIterator<K, V>::MapConstIterator(
    std::map<K, V>::const_iterator &&iter) {
  std::map<K, V>::const_iterator::operator=(std::move(iter));
}

template <typename K, typename V>
template <class Archive>
void MapConstIterator<K, V>::serialize(Archive &ar) {
  ar(cereal::binary_data(this, sizeof(*this)));
}

template <typename K, typename V>
MapConstReverseIterator<K, V>::MapConstReverseIterator() {}

template <typename K, typename V>
MapConstReverseIterator<K, V>::MapConstReverseIterator(
    std::map<K, V>::const_reverse_iterator &&iter) {
  std::map<K, V>::const_reverse_iterator::operator=(std::move(iter));
}

template <typename K, typename V>
template <class Archive>
void MapConstReverseIterator<K, V>::serialize(Archive &ar) {
  ar(cereal::binary_data(this, sizeof(*this)));
}

template <typename K, typename V>
Map<K, V>::Map(std::size_t capacity) {}

template <typename K, typename V>
Map<K, V>::Map(std::map<K, V> initial_state) : map_(std::move(initial_state)) {}

template <typename K, typename V>
std::size_t Map<K, V>::size() const {
  return map_.size();
}

template <typename K, typename V>
bool Map<K, V>::empty() const {
  return map_.empty();
}

template <typename K, typename V>
void Map<K, V>::clear() {
  map_.clear();
}

template <typename K, typename V>
void Map<K, V>::emplace(Key k, Val v) {
  map_.emplace(std::move(k), std::move(v));
}

template <typename K, typename V>
void Map<K, V>::emplace_back(Val v) {
  BUG();
}

template <typename K, typename V>
void Map<K, V>::merge(Map m) {
  for (auto &[k, v] : m.map_) {
    map_[std::move(k)] = std::move(v);
  }
}

template <typename K, typename V>
template <typename... S0s, typename... S1s>
void Map<K, V>::for_all(void (*fn)(const Key &key, Val &val, S0s...),
                        S1s &&... states) {
  for (auto &[k, v] : map_) {
    fn(k, v, states...);
  }
}
template <typename K, typename V>
std::optional<V> Map<K, V>::find_val(K k) {
  auto it = map_.find(std::move(k));
  if (it == map_.end()) {
    return std::nullopt;
  }
  return it->second;
}

template <typename K, typename V>
std::pair<typename Map<K, V>::Key, Map<K, V>> Map<K, V>::split() {
  assert(!map_.empty());

  auto mid = map_.size() / 2;
  auto split_it = map_.begin();
  std::advance(split_it, mid);

  auto latter_half_l_key = split_it->first;

  std::map<K, V> latter_half_map(std::make_move_iterator(split_it),
                                 std::make_move_iterator(map_.end()));
  map_.erase(split_it, map_.end());

  Map<K, V> latter_half_container(latter_half_map);
  return std::make_pair(latter_half_l_key, latter_half_container);
}

template <typename K, typename V>
Map<K, V>::ConstIterator Map<K, V>::cbegin() const {
  return map_.cbegin();
}

template <typename K, typename V>
Map<K, V>::ConstIterator Map<K, V>::cend() const {
  return map_.cend();
}

template <typename K, typename V>
Map<K, V>::ConstReverseIterator Map<K, V>::crbegin() const {
  return map_.crbegin();
}

template <typename K, typename V>
Map<K, V>::ConstReverseIterator Map<K, V>::crend() const {
  return map_.crend();
}

template <typename K, typename V>
template <class Archive>
void Map<K, V>::save(Archive &ar) const {
  ar(map_);
}

template <typename K, typename V>
template <class Archive>
void Map<K, V>::load(Archive &ar) {
  ar(map_);
}

template <typename K, typename V, typename LL>
V ShardedMap<K, V, LL>::operator[](const K &key) {
  auto found = this->find_val(key);
  if (found.has_value()) {
    return found.value();
  } else {
    auto default_val = V();
    this->emplace(key, default_val);
    return default_val;
  }
}

template <typename K, typename V, typename LL>
ShardedMap<K, V, LL>::ShardedMap(std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename K, typename V, typename LL>
ShardedMap<K, V, LL> make_sharded_map() {
  return ShardedMap<K, V, LL>(std::nullopt);
}

}  // namespace nu
