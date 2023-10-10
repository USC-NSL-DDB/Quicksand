#pragma once

#include <functional>
#include <optional>
#include <unordered_map>

#include "sharded_ds.hpp"

namespace nu {

template <class UMap>
struct UnorderedMapConstIterator : public UMap::const_iterator {
  constexpr static bool kContiguous = false;

  UnorderedMapConstIterator();
  UnorderedMapConstIterator(UMap::iterator &&iter);
  UnorderedMapConstIterator(UMap::const_iterator &&iter);
};

template <typename K, typename V, class H, BoolIntegral M>
class GeneralUnorderedMap {
 public:
  using Key = K;
  using Val = V;
  using UMap = std::conditional_t<M::value, std::unordered_multimap<K, V, H>,
                                  std::unordered_map<K, V, H>>;
  using ConstIterator = UnorderedMapConstIterator<UMap>;

  GeneralUnorderedMap() = default;
  GeneralUnorderedMap(const GeneralUnorderedMap &) = default;
  GeneralUnorderedMap &operator=(const GeneralUnorderedMap &) = default;
  GeneralUnorderedMap(GeneralUnorderedMap &&) noexcept = default;
  GeneralUnorderedMap &operator=(GeneralUnorderedMap &&) noexcept = default;

  std::size_t size() const;
  void reserve(std::size_t size);
  bool empty() const;
  void clear();
  std::size_t insert(Key k, Val v);
  bool erase(Key k);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&...states);
  ConstIterator find(Key k) const;
  std::optional<std::pair<K, V>> find_data(Key k) const;
  Val &operator[](Key k);
  void split(Key *mid_k, GeneralUnorderedMap *latter_half);
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

template <typename K, typename V, typename H, typename M, typename LL>
class GeneralShardedUnorderedMap;

template <typename K, typename V, typename H, typename LL>
using ShardedUnorderedMap =
    GeneralShardedUnorderedMap<K, V, H, std::false_type, LL>;

template <typename K, typename V, typename H, typename LL>
using ShardedUnorderedMultiMap =
    GeneralShardedUnorderedMap<K, V, H, std::true_type, LL>;

template <typename K, typename V, typename H, typename M, typename LL>
class GeneralShardedUnorderedMap
    : public ShardedDataStructure<
          GeneralLockedContainer<GeneralUnorderedMap<K, V, H, M>>, LL> {
 public:
  GeneralShardedUnorderedMap() = default;
  GeneralShardedUnorderedMap(const GeneralShardedUnorderedMap &) = default;
  GeneralShardedUnorderedMap &operator=(const GeneralShardedUnorderedMap &) =
      default;
  GeneralShardedUnorderedMap(GeneralShardedUnorderedMap &&) noexcept = default;
  GeneralShardedUnorderedMap &operator=(
      GeneralShardedUnorderedMap &&) noexcept = default;
  V operator[](const K &);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT apply_on(K k, RetT (*fn)(V *v, S0s...), S1s &&...states);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT apply_on(K k, RetT (*fn)(V &v, S0s...), S1s &&...states);

 private:
  using Base = ShardedDataStructure<
      GeneralLockedContainer<GeneralUnorderedMap<K, V, H, M>>, LL>;
  GeneralShardedUnorderedMap(
      std::optional<typename Base::ShardingHint> sharding_hint);

  friend class ProcletServer;
  template <typename K1, typename V1, typename H1, typename LL1>
  friend ShardedUnorderedMap<K1, V1, H1, LL1> make_sharded_unordered_map();
  template <typename K1, typename V1, typename H1, typename LL1>
  friend ShardedUnorderedMultiMap<K1, V1, H1, LL1>
  make_sharded_unordered_multimap();
};

template <typename K, typename V, typename H = std::hash<K>,
          typename LL = std::true_type>
ShardedUnorderedMap<K, V, H, LL> make_sharded_unordered_map();

template <typename K, typename V, typename H = std::hash<K>,
          typename LL = std::true_type>
ShardedUnorderedMultiMap<K, V, H, LL> make_sharded_unordered_multimap();

}  // namespace nu

#include "nu/impl/sharded_unordered_map.ipp"
