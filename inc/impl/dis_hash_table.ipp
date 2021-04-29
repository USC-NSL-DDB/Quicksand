#include <limits>

namespace nu {

template <typename K, typename V, typename Hash, typename KeyEqual>
DistributedHashTable<K, V, Hash, KeyEqual>::DistributedHashTable() {
  for (uint32_t i = 0; i < kNumShards; i++) {
    shards_[i] = std::move(RemObj<HashTableShard>::create());
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
  return shard.run(&HashTableShard::template get_with_hash<K1>, k, key_hash);
}

template <typename K, typename V, typename Hash, typename KeyEqual>
template <typename K1, typename V1>
void DistributedHashTable<K, V, Hash, KeyEqual>::put(K1 &&k, V1 &&v) {
  auto hash = Hash();
  auto key_hash = hash(k);
  auto shard_idx = get_shard_idx(key_hash);
  auto &shard = shards_[shard_idx];
  shard.run(&HashTableShard::template put_with_hash<K1, V1>, k, v, key_hash);
}

template <typename K, typename V, typename Hash, typename KeyEqual>
template <typename K1>
bool DistributedHashTable<K, V, Hash, KeyEqual>::remove(K1 &&k) {
  auto hash = Hash();
  auto key_hash = hash(k);
  auto shard_idx = get_shard_idx(key_hash);
  auto &shard = shards_[shard_idx];
  return shard.run(&HashTableShard::template remove_with_hash<K1>, k, key_hash);
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

} // namespace nu
