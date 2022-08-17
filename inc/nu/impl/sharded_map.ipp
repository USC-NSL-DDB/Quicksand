#include <cereal/types/map.hpp>

namespace nu {
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
void Map<K, V>::emplace_batch(Map &&m) {
  for (auto &[k, v] : map_) {
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

  std::map<K, V> latter_half_map(split_it, map_.end());
  map_.erase(split_it, map_.end());

  Map<K, V> latter_half_container(latter_half_map);
  return std::make_pair(latter_half_l_key, latter_half_container);
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

template <typename K, typename V, bool LowLat>
V ShardedMap<K, V, LowLat>::operator[](const K &key) {
  auto found = this->find_val(key);
  if (found.has_value()) {
    return found.value();
  } else {
    auto default_val = V();
    this->emplace(key, default_val);
    return default_val;
  }
}

template <typename K, typename V, bool LowLat>
ShardedMap<K, V, LowLat>::ShardedMap(std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename K, typename V, bool LowLat>
ShardedMap<K, V, LowLat> make_sharded_map() {
  return ShardedMap<K, V, LowLat>(std::nullopt);
}

}  // namespace nu
