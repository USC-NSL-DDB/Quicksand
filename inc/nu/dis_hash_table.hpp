#pragma once

#include <cereal/types/vector.hpp>
#include <memory>
#include <thread.h>
#include <utility>
#include <vector>

extern "C" {
#include <runtime/net.h>
}

#include "nu/rem_obj.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/spinlock.hpp"
#include "nu/utils/sync_hash_map.hpp"

namespace nu {

// TODO: support batch interface.
// TODO: support dynamic upsharding/downsharding.
template <typename K, typename V, typename Hash = std::hash<K>,
          typename KeyEqual = std::equal_to<K>, uint64_t NumBuckets = 32768>
class DistributedHashTable {
public:
  constexpr static uint32_t kDefaultPowerNumShards = 13;
  constexpr static uint64_t kNumBucketsPerShard = NumBuckets;

  using HashTableShard =
      SyncHashMap<NumBuckets, K, V, Hash, std::equal_to<K>,
                  std::allocator<std::pair<const K, V>>, Mutex>;
  struct Cap {
    std::vector<typename RemObj<HashTableShard>::Cap> shard_caps;

    template <class Archive> void serialize(Archive &ar) { ar(shard_caps); }
  };

  DistributedHashTable(const Cap &cap);
  DistributedHashTable(Cap &&cap);
  DistributedHashTable(const DistributedHashTable &) = delete;
  DistributedHashTable &operator=(const DistributedHashTable &) = delete;
  DistributedHashTable(DistributedHashTable &&);
  DistributedHashTable &operator=(DistributedHashTable &&);
  DistributedHashTable(uint32_t power_num_shards = kDefaultPowerNumShards,
                       bool pinned = false);
  DistributedHashTable(netaddr addr,
                       uint32_t power_num_shards = kDefaultPowerNumShards,
                       bool pinned = false);
  template <typename K1> std::optional<V> get(K1 &&k);
  template <typename K1> std::optional<V> get(K1 &&k, bool *is_local);
  template <typename K1, typename V1> void put(K1 &&k, V1 &&v);
  template <typename K1> bool remove(K1 &&k);
  template <typename K1, typename RetT, typename... A0s, typename... A1s>
  RetT apply(K1 &&k, RetT (*fn)(std::pair<const K, V> &, A0s...),
             A1s &&... args);
  template <typename K1> Future<std::optional<V>> get_async(K1 &&k);
  template <typename K1, typename V1> Future<void> put_async(K1 &&k, V1 &&v);
  template <typename K1> Future<bool> remove_async(K1 &&k);
  template <typename K1, typename RetT, typename... A0s, typename... A1s>
  Future<RetT> apply_async(K1 &&k, RetT (*fn)(std::pair<const K, V> &, A0s...),
                           A1s &&... args);
  Cap get_cap() const;
  template <typename RetT, typename... A0s, typename... A1s>
  RetT associative_reduce(
      RetT init_val,
      void (*reduced_fn)(RetT &, std::pair<const K, V> &, A0s...),
      void (*merge_fn)(RetT &result, RetT &partition, A0s...), A1s &&... args);
  template <typename RetT, typename... A0s, typename... A1s>
  std::vector<RetT> associative_reduce(
      RetT init_val,
      void (*reduced_fn)(RetT &, std::pair<const K, V> &, A0s...),
      A1s &&... args);
  std::vector<std::pair<K, V>> get_all_pairs();
  template <typename K1>
  static uint32_t get_shard_idx(K1 &&k, uint32_t power_num_shards);
  RemObjID get_shard_obj_id(uint32_t shard_id);

  // For debugging and performance analysis.
  template <typename K1>
  std::pair<std::optional<V>, uint32_t> get_with_ip(K1 &&k);

private:
  friend class Test;

  uint32_t get_shard_idx(uint64_t key_hash);

  uint32_t power_num_shards_;
  uint32_t num_shards_;
  std::unique_ptr<RemObj<HashTableShard>[]> shards_;
};

} // namespace nu

#include "nu/impl/dis_hash_table.ipp"
