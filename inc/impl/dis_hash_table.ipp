#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>
#include <limits>

namespace nu {

template <typename K, typename V, typename Hash, typename KeyEqual>
DistributedHashTable<K, V, Hash, KeyEqual>::DistributedHashTable(
    DistributedHashTable &&o) {
  *this = std::move(o);
}

template <typename K, typename V, typename Hash, typename KeyEqual>
DistributedHashTable<K, V, Hash, KeyEqual> &
DistributedHashTable<K, V, Hash, KeyEqual>::operator=(
    DistributedHashTable &&o) {
  for (uint32_t i = 0; i < kNumShards; i++) {
    shards_[i] = std::move(o.shards_[i]);
  }
  return *this;
}

template <typename K, typename V, typename Hash, typename KeyEqual>
DistributedHashTable<K, V, Hash, KeyEqual>::DistributedHashTable(
    const Cap &cap) {
  for (uint32_t i = 0; i < kNumShards; i++) {
    shards_[i] = std::move(RemObj<HashTableShard>(cap.shard_caps[i]));
  }
}

template <typename K, typename V, typename Hash, typename KeyEqual>
DistributedHashTable<K, V, Hash, KeyEqual>::DistributedHashTable(bool pinned) {
  for (uint32_t i = 0; i < kNumShards; i++) {
    if (pinned) {
      shards_[i] = std::move(RemObj<HashTableShard>::create_pinned());
    } else {
      shards_[i] = std::move(RemObj<HashTableShard>::create());
    }
  }
}

template <typename K, typename V, typename Hash, typename KeyEqual>
DistributedHashTable<K, V, Hash, KeyEqual>::DistributedHashTable(netaddr addr,
                                                                 bool pinned) {
  for (uint32_t i = 0; i < kNumShards; i++) {
    if (pinned) {
      shards_[i] = std::move(RemObj<HashTableShard>::create_pinned_at(addr));
    } else {
      shards_[i] = std::move(RemObj<HashTableShard>::create_at(addr));
    }
  }
}

template <typename K, typename V, typename Hash, typename KeyEqual>
uint32_t
DistributedHashTable<K, V, Hash, KeyEqual>::get_shard_idx(uint64_t key_hash) {
  return key_hash / (std::numeric_limits<uint64_t>::max() / kNumShards);
}

template <typename K, typename V, typename Hash, typename KeyEqual>
template <typename K1>
std::optional<V> DistributedHashTable<K, V, Hash, KeyEqual>::get(K1 &&k) {
  auto hash = Hash();
  auto key_hash = hash(k);
  auto shard_idx = get_shard_idx(key_hash);
  auto &shard = shards_[shard_idx];
  return shard.__run(&HashTableShard::template get_with_hash<K1>, k, key_hash);
}

template <typename K, typename V, typename Hash, typename KeyEqual>
template <typename K1>
std::pair<std::optional<V>, uint32_t>
DistributedHashTable<K, V, Hash, KeyEqual>::get_with_ip(K1 &&k) {
  auto hash = Hash();
  auto key_hash = hash(k);
  auto shard_idx = get_shard_idx(key_hash);
  auto &shard = shards_[shard_idx];
  return shard.__run(
      +[](HashTableShard &shard, const K &k, uint64_t key_hash) {
        return std::make_pair(shard.get_with_hash(k, key_hash), get_cfg_ip());
      },
      k, key_hash);
}

template <typename K, typename V, typename Hash, typename KeyEqual>
template <typename K1, typename V1>
void DistributedHashTable<K, V, Hash, KeyEqual>::put(K1 &&k, V1 &&v) {
  auto hash = Hash();
  auto key_hash = hash(k);
  auto shard_idx = get_shard_idx(key_hash);
  auto &shard = shards_[shard_idx];
  shard.__run(&HashTableShard::template put_with_hash<K1, V1>, k, v, key_hash);
}

template <typename K, typename V, typename Hash, typename KeyEqual>
template <typename K1>
bool DistributedHashTable<K, V, Hash, KeyEqual>::remove(K1 &&k) {
  auto hash = Hash();
  auto key_hash = hash(k);
  auto shard_idx = get_shard_idx(key_hash);
  auto &shard = shards_[shard_idx];
  return shard.__run(&HashTableShard::template remove_with_hash<K1>, k,
                     key_hash);
}

template <typename K, typename V, typename Hash, typename KeyEqual>
template <typename K1>
Future<std::optional<V>>
DistributedHashTable<K, V, Hash, KeyEqual>::get_async(K1 &&k) {
  auto *promise = Promise<bool>::create([&, k] { return get(k); });
  return promise->get_future();
}

template <typename K, typename V, typename Hash, typename KeyEqual>
template <typename K1, typename V1>
Future<void> DistributedHashTable<K, V, Hash, KeyEqual>::put_async(K1 &&k,
                                                                   V1 &&v) {
  auto *promise = Promise<bool>::create([&, k, v] { return put(k, v); });
  return promise->get_future();
}

template <typename K, typename V, typename Hash, typename KeyEqual>
template <typename K1>
Future<bool> DistributedHashTable<K, V, Hash, KeyEqual>::remove_async(K1 &&k) {
  auto *promise = Promise<bool>::create([&, k] { return remove(k); });
  return promise->get_future();
}

template <typename K, typename V, typename Hash, typename KeyEqual>
DistributedHashTable<K, V, Hash, KeyEqual>::Cap
DistributedHashTable<K, V, Hash, KeyEqual>::get_cap() {
  Cap cap;
  for (uint32_t i = 0; i < kNumShards; i++) {
    cap.shard_caps[i] = shards_[i].get_cap();
  }
  return cap;
}

} // namespace nu
