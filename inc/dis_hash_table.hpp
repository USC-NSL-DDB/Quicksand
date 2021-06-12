#pragma once

#include <utility>

extern "C" {
#include <runtime/net.h>
}

#include "rem_obj.hpp"
#include "utils/spinlock.hpp"
#include "utils/sync_hash_map.hpp"

namespace nu {

// TODO: support batch interface.
// TODO: support dynamic upsharding/downsharding.
template <typename K, typename V, typename Hash = std::hash<K>,
          typename KeyEqual = std::equal_to<K>>
class DistributedHashTable {
public:
  constexpr static uint32_t kNumShards = 8192;
  constexpr static uint32_t kNumBucketsPerShard = 65536;

  using HashTableShard =
      SyncHashMap<kNumBucketsPerShard, K, V, Hash, std::equal_to<K>,
                  std::allocator<std::pair<const K, V>>, SpinLock>;
  struct Cap {
    RemObj<HashTableShard>::Cap shard_caps[kNumShards];
  };

  DistributedHashTable(const Cap &cap);
  DistributedHashTable(const DistributedHashTable &) = delete;
  DistributedHashTable &operator=(const DistributedHashTable &) = delete;
  DistributedHashTable(DistributedHashTable &&);
  DistributedHashTable &operator=(DistributedHashTable &&);
  DistributedHashTable(bool pinned = false);
  DistributedHashTable(netaddr addr, bool pinned = false);
  template <typename K1> std::optional<V> get(K1 &&k);
  template <typename K1, typename V1> void put(K1 &&k, V1 &&v);
  template <typename K1> bool remove(K1 &&k);
  template <typename K1> Future<std::optional<V>> get_async(K1 &&k);
  template <typename K1, typename V1> Future<void> put_async(K1 &&k, V1 &&v);
  template <typename K1> Future<bool> remove_async(K1 &&k);
  Cap get_cap();

  // For debugging and performance analysis.
  template <typename K1>
  std::pair<std::optional<V>, uint32_t> get_with_ip(K1 &&k);

private:
  friend class Test;

  uint32_t get_shard_idx(uint64_t key_hash);

  RemObj<HashTableShard> shards_[kNumShards];
};

} // namespace nu

#include "impl/dis_hash_table.ipp"
