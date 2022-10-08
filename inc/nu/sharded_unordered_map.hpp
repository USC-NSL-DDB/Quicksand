#pragma once

#include <unordered_map>

#include "sharded_ds.hpp"

namespace nu {

template <class UMap>
struct UnorderedMapConstIterator : public UMap::const_iterator {
  UnorderedMapConstIterator();
  UnorderedMapConstIterator(UMap::iterator &&iter);
  UnorderedMapConstIterator(UMap::const_iterator &&iter);
};

template <typename K, typename V, BoolIntegral M>
class GeneralUnorderedMap {
 public:
  using Key = K;
  using Val = V;
  using UMap = std::conditional_t<M::value, std::unordered_multimap<K, V>,
                                  std::unordered_map<K, V>>;
  using ConstIterator = UnorderedMapConstIterator<UMap>;

  GeneralUnorderedMap() = default;
  GeneralUnorderedMap(std::optional<Key> l_key);
  GeneralUnorderedMap(std::optional<Key> l_key, std::size_t capacity);
  GeneralUnorderedMap(const GeneralUnorderedMap &) = default;
  GeneralUnorderedMap &operator=(const GeneralUnorderedMap &) = default;
  GeneralUnorderedMap(GeneralUnorderedMap &&) noexcept = default;
  GeneralUnorderedMap &operator=(GeneralUnorderedMap &&) noexcept = default;

  std::size_t size() const;
  void reserve(std::size_t size);
  bool empty() const;
  void clear();
  void emplace(Key k, Val v);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  ConstIterator find(Key k);
  std::pair<Key, GeneralUnorderedMap> split();
  void merge(GeneralUnorderedMap m);
  ConstIterator cbegin() const;
  ConstIterator cend() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  GeneralUnorderedMap(UMap initial_state);

  UMap map_;
};

template <typename K, typename V, typename M, typename LL>
class GeneralShardedUnorderedMap;

template <typename K, typename V, typename LL>
using ShardedUnorderedMap =
    GeneralShardedUnorderedMap<K, V, std::false_type, LL>;

template <typename K, typename V, typename LL>
using ShardedUnorderedMultiMap =
    GeneralShardedUnorderedMap<K, V, std::true_type, LL>;

template <typename K, typename V, typename M, typename LL>
class GeneralShardedUnorderedMap
    : public ShardedDataStructure<
          GeneralLockedContainer<GeneralUnorderedMap<K, V, M>>, LL> {
 public:
  GeneralShardedUnorderedMap(const GeneralShardedUnorderedMap &) = default;
  GeneralShardedUnorderedMap &operator=(const GeneralShardedUnorderedMap &) =
      default;
  GeneralShardedUnorderedMap(GeneralShardedUnorderedMap &&) noexcept = default;
  GeneralShardedUnorderedMap &operator=(
      GeneralShardedUnorderedMap &&) noexcept = default;
  V operator[](const K &);

 private:
  using Base =
      ShardedDataStructure<GeneralLockedContainer<GeneralUnorderedMap<K, V, M>>,
                           LL>;
  GeneralShardedUnorderedMap() = default;
  GeneralShardedUnorderedMap(std::optional<typename Base::Hint> hint);

  friend class ProcletServer;
  template <typename K1, typename V1, typename LL1>
  friend ShardedUnorderedMap<K1, V1, LL1> make_sharded_unordered_map();
  template <typename K1, typename V1, typename LL1>
  friend ShardedUnorderedMultiMap<K1, V1, LL1>
  make_sharded_unordered_multimap();
};

template <typename K, typename V, typename LL>
ShardedUnorderedMap<K, V, LL> make_sharded_unordered_map();

template <typename K, typename V, typename LL>
ShardedUnorderedMultiMap<K, V, LL> make_sharded_unordered_multimap();

}  // namespace nu

#include "nu/impl/sharded_unordered_map.ipp"
