#pragma once

#include <map>

#include "sharded_ds.hpp"

namespace nu {

template <class Map>
struct MapConstIterator : public Map::const_iterator {
  MapConstIterator();
  MapConstIterator(Map::iterator &&iter);
  MapConstIterator(Map::const_iterator &&iter);
  template <class Archive>
  void serialize(Archive &ar);
};

template <class Map>
struct MapConstReverseIterator : public Map::const_reverse_iterator {
  MapConstReverseIterator();
  MapConstReverseIterator(Map::reverse_iterator &&iter);
  MapConstReverseIterator(Map::const_reverse_iterator &&iter);
  template <class Archive>
  void serialize(Archive &ar);
};

template <typename K, typename V, BoolIntegral M>
class GeneralMap {
 public:
  using Key = K;
  using Val = V;
  using Map = std::conditional_t<M::value, std::multimap<K, V>, std::map<K, V>>;
  using ConstIterator = MapConstIterator<Map>;
  using ConstReverseIterator = MapConstReverseIterator<Map>;

  GeneralMap() = default;
  GeneralMap(std::optional<Key> l_key);
  GeneralMap(std::optional<Key> l_key, std::size_t capacity);
  GeneralMap(const GeneralMap &) = default;
  GeneralMap &operator=(const GeneralMap &) = default;
  GeneralMap(GeneralMap &&) noexcept = default;
  GeneralMap &operator=(GeneralMap &&) noexcept = default;

  std::size_t size() const;
  bool empty() const;
  void clear();
  void emplace(Key k, Val v);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  ConstIterator find(Key k);
  std::pair<Key, GeneralMap> split();
  void merge(GeneralMap m);
  ConstIterator cbegin() const;
  ConstIterator cend() const;
  ConstReverseIterator crbegin() const;
  ConstReverseIterator crend() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  GeneralMap(Map initial_state);

  Map map_;
};

template <typename K, typename V, BoolIntegral M, typename LL>
class GeneralShardedMap;

template <typename K, typename V, typename LL>
using ShardedMap = GeneralShardedMap<K, V, std::false_type, LL>;

template <typename K, typename V, typename LL>
using ShardedMultiMap = GeneralShardedMap<K, V, std::true_type, LL>;

template <typename K, typename V, BoolIntegral M, typename LL>
class GeneralShardedMap
    : public ShardedDataStructure<GeneralLockedContainer<GeneralMap<K, V, M>>,
                                  LL> {
 public:
  GeneralShardedMap(const GeneralShardedMap &) = default;
  GeneralShardedMap &operator=(const GeneralShardedMap &) = default;
  GeneralShardedMap(GeneralShardedMap &&) noexcept = default;
  GeneralShardedMap &operator=(GeneralShardedMap &&) noexcept = default;

  V operator[](const K &);

 private:
  using Base =
      ShardedDataStructure<GeneralLockedContainer<GeneralMap<K, V, M>>, LL>;
  GeneralShardedMap() = default;
  GeneralShardedMap(std::optional<typename Base::Hint> hint);

  friend class ProcletServer;
  template <typename K1, typename V1, typename LL1>
  friend ShardedMap<K1, V1, LL1> make_sharded_map();
  template <typename K1, typename V1, typename LL1>
  friend ShardedMultiMap<K1, V1, LL1> make_sharded_multi_map();
};

template <typename K, typename V, typename LL>
ShardedMap<K, V, LL> make_sharded_map();

template <typename K, typename V, typename LL>
ShardedMultiMap<K, V, LL> make_sharded_multi_map();

}  // namespace nu

#include "nu/impl/sharded_map.ipp"
