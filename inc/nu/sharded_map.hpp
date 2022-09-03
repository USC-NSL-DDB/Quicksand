#pragma once

#include <map>

#include "sharded_ds.hpp"

namespace nu {
template <typename K, typename V>
struct MapConstIterator : public std::map<K, V>::const_iterator {
  MapConstIterator();
  MapConstIterator(std::map<K, V>::const_iterator &&iter);
  template <class Archive>
  void serialize(Archive &ar);
};

template <typename K, typename V>
struct MapConstReverseIterator : public std::map<K, V>::const_reverse_iterator {
  MapConstReverseIterator();
  MapConstReverseIterator(std::map<K, V>::const_reverse_iterator &&iter);
  template <class Archive>
  void serialize(Archive &ar);
};

template <typename K, typename V>
class Map {
 public:
  using Key = K;
  using Val = V;
  using IterVal = std::pair<Key, Val>;
  using ConstIterator = MapConstIterator<K, V>;
  using ConstReverseIterator = MapConstReverseIterator<K, V>;

  Map() = default;
  Map(std::size_t capacity);
  Map(const Map &) = default;
  Map &operator=(const Map &) = default;
  Map(Map &&) noexcept = default;
  Map &operator=(Map &&) noexcept = default;

  std::size_t size() const;
  bool empty() const;
  void clear();
  void emplace(Key k, Val v);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  std::optional<Val> find_val(Key k);
  std::pair<Key, Map> split();
  void merge(Map m);
  ConstIterator cbegin() const;
  ConstIterator cend() const;
  ConstReverseIterator crbegin() const;
  ConstReverseIterator crend() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  Map(std::map<K, V> initial_state);

  std::map<K, V> map_;
};

template <typename K, typename V, typename LL>
class ShardedMap
    : public ShardedDataStructure<GeneralLockedContainer<Map<K, V>>, LL> {
 public:
  ShardedMap(const ShardedMap &) = default;
  ShardedMap &operator=(const ShardedMap &) = default;
  ShardedMap(ShardedMap &&) noexcept = default;
  ShardedMap &operator=(ShardedMap &&) noexcept = default;

  V operator[](const K &);

 private:
  using Base = ShardedDataStructure<GeneralLockedContainer<Map<K, V>>, LL>;
  ShardedMap() = default;
  ShardedMap(std::optional<typename Base::Hint> hint);

  friend class ProcletServer;
  template <typename K1, typename V1, typename LL1>
  friend ShardedMap<K1, V1, LL1> make_sharded_map();
};

template <typename K, typename V, typename LL>
ShardedMap<K, V, LL> make_sharded_map();
}  // namespace nu

#include "nu/impl/sharded_map.ipp"
