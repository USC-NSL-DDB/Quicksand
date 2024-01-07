#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>

#include "commons.hpp"
#include "sharded_ds.hpp"
#include "utils/mutex.hpp"
#include "utils/sync_hash_map.hpp"

namespace nu {

template <typename K, typename V, class H>
class GeneralTSUMap {
 public:
  using Key = std::size_t;
  using Val = DIPair<K, V>;
  using TSUMap =
      SyncHashMap</* NumBuckets = */ 4096, K, V, H, std::equal_to<K>,
                  std::allocator<std::pair<const K, V>>, Mutex>;
  using ConstIterator = V *;

  GeneralTSUMap() = default;
  GeneralTSUMap(const GeneralTSUMap &) = default;
  GeneralTSUMap &operator=(const GeneralTSUMap &) = default;
  GeneralTSUMap(GeneralTSUMap &&) noexcept = default;
  GeneralTSUMap &operator=(GeneralTSUMap &&) noexcept = default;

  std::size_t size() const;
  bool empty() const;
  std::size_t insert(std::size_t hash, K k, V v);
  std::optional<V> find_data(std::size_t hash, K k) const;
  bool erase(std::size_t hash, K k);
  void split(Key *mid_k, GeneralTSUMap *latter_half);
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  GeneralTSUMap(TSUMap initial_state);
  template <typename K1, typename V1, typename H1>
  friend class ShardedTSUMap;

  TSUMap map_;
};

template <typename K, typename V, typename H>
class ShardedTSUMap
    : public ShardedDataStructure<GeneralContainer<GeneralTSUMap<K, V, H>>,
                                  /* LL = */ std::true_type> {
 public:
  ShardedTSUMap() = default;
  ShardedTSUMap(const ShardedTSUMap &) = default;
  ShardedTSUMap &operator=(const ShardedTSUMap &) = default;
  ShardedTSUMap(ShardedTSUMap &&) noexcept = default;
  ShardedTSUMap &operator=(ShardedTSUMap &&) noexcept = default;
  template <typename RetT, typename... S0s, typename... S1s>
  RetT apply_on(K k, RetT (*fn)(std::pair<const K, V> &, S0s...),
                S1s &&...states);
  std::optional<V> find_data(K k) const;
  bool erase(const K &k);
  template <typename K1, typename V1>
  void insert(K1 &&k, V1 &&v);

 private:
  using Base = ShardedDataStructure<GeneralContainer<GeneralTSUMap<K, V, H>>,
                                    std::true_type>;
  ShardedTSUMap(std::optional<typename Base::ShardingHint> sharding_hint);

  H hasher_;
  friend class ProcletServer;
  template <typename K1, typename V1, typename H1>
  friend ShardedTSUMap<K1, V1, H1> make_sharded_ts_umap(uint32_t num_shards);
};

template <typename K, typename V, typename H = std::hash<K>>
ShardedTSUMap<K, V, H> make_sharded_ts_umap(uint32_t num_shards = 1);

}  // namespace nu

#include "nu/impl/sharded_ts_umap.ipp"
