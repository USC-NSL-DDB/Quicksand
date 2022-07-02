#pragma once

#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <optional>
#include <queue>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "nu/commons.hpp"
#include "nu/rpc_client_mgr.hpp"
#include "nu/utils/future.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/reader_writer_lock.hpp"
#include "nu/utils/scoped_lock.hpp"
#include "nu/utils/span_to_vector.hpp"

namespace nu {

template <typename K, typename V>
class ShardedPairCollection {
 public:
  using PairType = std::pair<K, V>;
  using ShardDataType = std::vector<PairType>;
  constexpr static uint32_t kDefaultMaxShardBytes = 2 << 20;
  constexpr static uint32_t kDefaultMaxCacheBytes = 100 << 10;

  ShardedPairCollection();
  ShardedPairCollection(const ShardedPairCollection &);
  ShardedPairCollection &operator=(const ShardedPairCollection &);
  ShardedPairCollection(ShardedPairCollection &&) noexcept;
  ShardedPairCollection &operator=(ShardedPairCollection &&) noexcept;
  ~ShardedPairCollection();
  template <typename K1, typename V1>
  void emplace(K1 &&k1, V1 &&v1);
  void emplace(PairType &&p);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(std::pair<const K, V> &, S0s...), S1s &&... states);
  ShardDataType collect();
  void flush();
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  class ShardingMapping;
  class Shard {
   public:
    Shard(WeakProclet<ShardingMapping> mapping, uint32_t max_shard_size,
          std::optional<K> l_key, std::optional<K> r_key, ShardDataType data);
    Shard(WeakProclet<ShardingMapping> mapping, uint32_t max_shard_size,
          std::optional<K> l_key, std::optional<K> r_key,
          SpanToVectorWrapper<PairType> data);
    ShardDataType get_data();
    std::pair<ScopedLock<Mutex>, ShardDataType *> get_data_ptr();
    bool try_emplace_back(std::optional<K> l_key, std::optional<K> r_key,
                          ShardDataType p);

   private:
    uint32_t max_shard_size_;
    WeakProclet<ShardingMapping> mapping_;
    std::optional<K> l_key_;
    std::optional<K> r_key_;
    ShardDataType data_;
    Mutex mutex_;
  };

  class ShardingMapping {
   public:
    ShardingMapping();
    std::vector<std::pair<std::optional<K>, WeakProclet<Shard>>>
    get_shards_in_range(std::optional<K> l_key, std::optional<K> r_key);
    std::vector<WeakProclet<Shard>> get_all_shards();
    void update_mapping(std::optional<K> k, Proclet<Shard> shard);

   private:
    std::map<std::optional<K>, Proclet<Shard>, std::greater<std::optional<K>>>
        mapping_;
    ReaderWriterLock rw_lock_;
  };

  struct Cache {
    WeakProclet<Shard> shard;
    ShardDataType data;
    uint32_t *per_ip_cache_size = nullptr;

    Cache();
    Cache(WeakProclet<Shard>);

    template <class Archive>
    void save(Archive &ar) const;
    template <class Archive>
    void load(Archive &ar);
  };

  struct PushDataReq {
    std::optional<K> l_key;
    std::optional<K> r_key;
    WeakProclet<Shard> shard;
    ShardDataType data;

    template <class Archive>
    void serialize(Archive &ar);
  };

  using KeyToCacheMappingType =
      std::map<std::optional<K>, Cache, std::greater<std::optional<K>>>;
  using IPToCachesMappingType = std::unordered_map<
      NodeIP,
      std::pair<uint32_t, std::list<typename KeyToCacheMappingType::iterator>>>;

  Proclet<ShardingMapping> mapping_;
  uint32_t max_shard_size_;
  uint32_t max_cache_size_;
  KeyToCacheMappingType key_to_cache_;
  IPToCachesMappingType ip_to_caches_;
  std::map<NodeIP, Proclet<ErasedType>> node_proxy_shards_;
  ReaderWriterLock rw_lock_;

  ShardedPairCollection(uint32_t max_shard_bytes, uint32_t max_cache_bytes,
                        std::optional<K> initial_l_key,
                        std::optional<K> initial_r_key);
  ShardedPairCollection(uint64_t num, K estimated_min_key,
                        std::function<void(K &, uint64_t)> key_inc_fn,
                        uint32_t max_shard_bytes, uint32_t max_cache_bytes);
  bool push_data(NodeIP ip, std::vector<PushDataReq> reqs);
  void add_cache(std::optional<K> k, WeakProclet<Shard> shard);
  void bind_cache(NodeIP, const KeyToCacheMappingType::iterator &cache);
  std::vector<PushDataReq> gen_push_data_reqs(NodeIP ip);
  WeakProclet<ErasedType> get_node_proxy_shard(NodeIP ip);
  template <typename K1, typename V1>
  friend ShardedPairCollection<K1, V1> make_sharded_pair_collection(
      uint32_t max_shard_bytes, uint32_t max_cache_bytes);
  template <typename K1, typename V1>
  friend ShardedPairCollection<K1, V1> make_sharded_pair_collection(
      uint64_t num, K1 estimated_min_key,
      std::function<void(K1 &, uint64_t)> key_inc_fn, uint32_t max_shard_bytes,
      uint32_t max_cache_bytes);
};

template <typename K, typename V>
ShardedPairCollection<K, V> make_sharded_pair_collection(
    uint32_t max_shard_bytes =
        ShardedPairCollection<K, V>::kDefaultMaxShardBytes,
    uint32_t max_cache_bytes =
        ShardedPairCollection<K, V>::kDefaultMaxCacheBytes);

template <typename K, typename V>
ShardedPairCollection<K, V> make_sharded_pair_collection(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn,
    uint32_t max_shard_bytes =
        ShardedPairCollection<K, V>::kDefaultMaxShardBytes,
    uint32_t max_cache_bytes =
        ShardedPairCollection<K, V>::kDefaultMaxCacheBytes);

}  // namespace nu

#include "nu/impl/sharded_pair_collect.ipp"
