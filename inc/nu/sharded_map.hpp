#pragma once

#include "sharded_ds.hpp"

namespace nu {
template <typename K, typename V>
class Map {
 public:
  using Key = K;
  using Val = V;

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
  void emplace_batch(Map &&m);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  std::optional<Val> find_val(Key k);
  std::pair<Key, Map> split();
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
    : public ShardedDataStructure<GeneralContainer<Map<K, V>>, LL> {
 public:
  ShardedMap() = default;
  ShardedMap(const ShardedMap &) = default;
  ShardedMap &operator=(const ShardedMap &) = default;
  ShardedMap(ShardedMap &&) noexcept = default;
  ShardedMap &operator=(ShardedMap &&) noexcept = default;

  V operator[](const K &);

 private:
  using Base = ShardedDataStructure<GeneralContainer<Map<K, V>>, LL>;
  ShardedMap(std::optional<typename Base::Hint> hint);

  template <typename K1, typename V1, typename LL1>
  friend ShardedMap<K1, V1, LL1> make_sharded_map();
};

template <typename K, typename V, typename LL>
ShardedMap<K, V, LL> make_sharded_map();
}  // namespace nu

#include "nu/impl/sharded_map.ipp"
