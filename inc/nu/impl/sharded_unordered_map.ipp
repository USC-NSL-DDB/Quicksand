#include <cereal/types/unordered_map.hpp>
#include <utility>

#include "nu/utils/bfprt/median_of_ninthers.h"

namespace nu {

template <typename K, typename V>
UnorderedMapConstIterator<K, V>::UnorderedMapConstIterator() {}

template <typename K, typename V>
UnorderedMapConstIterator<K, V>::UnorderedMapConstIterator(
    std::unordered_map<K, V>::iterator &&iter) {
  std::unordered_map<K, V>::const_iterator::operator=(std::move(iter));
}

template <typename K, typename V>
UnorderedMapConstIterator<K, V>::UnorderedMapConstIterator(
    std::unordered_map<K, V>::const_iterator &&iter) {
  std::unordered_map<K, V>::const_iterator::operator=(std::move(iter));
}

template <typename K, typename V>
template <class Archive>
void UnorderedMapConstIterator<K, V>::serialize(Archive &ar) {
  ar(cereal::binary_data(this, sizeof(*this)));
}

template <typename K, typename V>
UnorderedMap<K, V>::UnorderedMap(std::size_t capacity) {
  map_.reserve(capacity);
}

template <typename K, typename V>
UnorderedMap<K, V>::UnorderedMap(std::unordered_map<K, V> initial_state)
    : map_(std::move(initial_state)) {}

template <typename K, typename V>
std::size_t UnorderedMap<K, V>::size() const {
  return map_.size();
}

template <typename K, typename V>
bool UnorderedMap<K, V>::empty() const {
  return map_.empty();
}

template <typename K, typename V>
void UnorderedMap<K, V>::clear() {
  map_.clear();
}

template <typename K, typename V>
void UnorderedMap<K, V>::emplace(Key k, Val v) {
  map_.emplace(std::move(k), std::move(v));
}

template <typename K, typename V>
void UnorderedMap<K, V>::merge(UnorderedMap m) {
  for (auto &[k, v] : m.map_) {
    map_[std::move(k)] = std::move(v);
  }
}

template <typename K, typename V>
template <typename... S0s, typename... S1s>
void UnorderedMap<K, V>::for_all(void (*fn)(const Key &key, Val &val, S0s...),
                                 S1s &&... states) {
  for (auto &[k, v] : map_) {
    fn(k, v, states...);
  }
}
template <typename K, typename V>
UnorderedMap<K, V>::ConstIterator UnorderedMap<K, V>::find(K k) {
  return map_.find(std::move(k));
}

template <typename K, typename V>
std::pair<typename UnorderedMap<K, V>::Key, UnorderedMap<K, V>>
UnorderedMap<K, V>::split() {
  assert(!map_.empty());

  std::vector<K> keys;
  keys.reserve(map_.size());
  for (auto &[k, v] : map_) {
    keys.push_back(k);
  }

  std::sort(keys.begin(), keys.end());
  auto mid_key = keys[keys.size() / 2];

  std::unordered_map<K, V> latter_half_map;
  for (auto it = map_.cbegin(); it != map_.cend();) {
    if (it->second >= mid_key) {
      latter_half_map.emplace(std::move(it->first), std::move(it->second));
      map_.erase(it++);
    } else {
      ++it;
    }
  }

  UnorderedMap<K, V> latter_half_container(latter_half_map);

  return std::make_pair(mid_key, latter_half_container);
}

template <typename K, typename V>
UnorderedMap<K, V>::ConstIterator UnorderedMap<K, V>::cbegin() const {
  return map_.cbegin();
}

template <typename K, typename V>
UnorderedMap<K, V>::ConstIterator UnorderedMap<K, V>::cend() const {
  return map_.cend();
}

template <typename K, typename V>
template <class Archive>
void UnorderedMap<K, V>::save(Archive &ar) const {
  ar(map_);
}

template <typename K, typename V>
template <class Archive>
void UnorderedMap<K, V>::load(Archive &ar) {
  ar(map_);
}

template <typename K, typename V, typename LL>
V ShardedUnorderedMap<K, V, LL>::operator[](const K &key) {
  auto found = this->find_val(key);
  if (found.has_value()) {
    return found->second;
  } else {
    auto default_val = V();
    this->emplace(key, default_val);
    return default_val;
  }
}

template <typename K, typename V, typename LL>
ShardedUnorderedMap<K, V, LL>::ShardedUnorderedMap(
    std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename K, typename V, typename LL>
ShardedUnorderedMap<K, V, LL> make_sharded_unordered_map() {
  return ShardedUnorderedMap<K, V, LL>(std::nullopt);
}
}  // namespace nu
