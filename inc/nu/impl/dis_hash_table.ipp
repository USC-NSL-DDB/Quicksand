#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>

#include "nu/commons.hpp"

namespace nu {

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::DistributedHashTable(
    DistributedHashTable &&o) {
  *this = std::move(o);
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets> &
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::operator=(
    DistributedHashTable &&o) {
  power_num_shards_ = o.power_num_shards_;
  num_shards_ = o.num_shards_;
  shards_ = std::make_unique<RemObj<HashTableShard>[]>(num_shards_);
  for (uint32_t i = 0; i < num_shards_; i++) {
    shards_[i] = std::move(o.shards_[i]);
  }
  return *this;
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::DistributedHashTable(
    const Cap &cap)
    : power_num_shards_(bsr_64(cap.shard_caps.size())),
      num_shards_(cap.shard_caps.size()) {
  shards_ = std::make_unique<RemObj<HashTableShard>[]>(num_shards_);
  for (uint32_t i = 0; i < num_shards_; i++) {
    std::construct_at(&shards_[i], cap.shard_caps[i]);
  }
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::DistributedHashTable(
    Cap &&cap)
    : power_num_shards_(bsr_64(cap.shard_caps.size())),
      num_shards_(cap.shard_caps.size()) {
  shards_ = std::make_unique<RemObj<HashTableShard>[]>(num_shards_);
  for (uint32_t i = 0; i < num_shards_; i++) {
    std::construct_at(&shards_[i], cap.shard_caps[i]);
  }
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::DistributedHashTable(
    uint32_t power_num_shards, bool pinned)
    : power_num_shards_(power_num_shards), num_shards_(1 << power_num_shards_) {
  shards_ = std::make_unique<RemObj<HashTableShard>[]>(num_shards_);
  for (uint32_t i = 0; i < num_shards_; i++) {
    if (pinned) {
      shards_[i] = std::move(RemObj<HashTableShard>::create_pinned());
    } else {
      shards_[i] = std::move(RemObj<HashTableShard>::create());
    }
  }
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::DistributedHashTable(
    netaddr addr, uint32_t power_num_shards, bool pinned)
    : power_num_shards_(power_num_shards), num_shards_(1 << power_num_shards_) {
  shards_ = std::make_unique<RemObj<HashTableShard>[]>(num_shards_);
  for (uint32_t i = 0; i < num_shards_; i++) {
    if (pinned) {
      shards_[i] = std::move(RemObj<HashTableShard>::create_pinned_at(addr));
    } else {
      shards_[i] = std::move(RemObj<HashTableShard>::create_at(addr));
    }
  }
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
uint32_t DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::get_shard_idx(
    uint64_t key_hash) {
  return key_hash / (std::numeric_limits<uint64_t>::max() >> power_num_shards_);
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
template <typename K1>
std::optional<V>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::get(K1 &&k) {
  auto hash = Hash();
  auto key_hash = hash(std::forward<K1>(k));
  auto shard_idx = get_shard_idx(key_hash);
  auto &shard = shards_[shard_idx];
  return shard.__run(&HashTableShard::template get_copy_with_hash<K1>,
                     std::forward<K1>(k), key_hash);
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
template <typename K1>
std::pair<std::optional<V>, uint32_t>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::get_with_ip(K1 &&k) {
  auto hash = Hash();
  auto key_hash = hash(std::forward<K1>(k));
  auto shard_idx = get_shard_idx(key_hash);
  auto &shard = shards_[shard_idx];
  return shard.__run(
      +[](HashTableShard &shard, K1 &&k, uint64_t key_hash) {
        return std::make_pair(
            shard.get_copy_with_hash(std::forward<K1>(k), key_hash),
            get_cfg_ip());
      },
      std::forward<K1>(k), key_hash);
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
template <typename K1, typename V1>
void DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::put(K1 &&k,
                                                                 V1 &&v) {
  auto hash = Hash();
  auto key_hash = hash(std::forward<K1>(k));
  auto shard_idx = get_shard_idx(key_hash);
  auto &shard = shards_[shard_idx];
  shard.__run(&HashTableShard::template put_with_hash<K1, V1>,
              std::forward<K1>(k), std::forward<V1>(v), key_hash);
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
template <typename K1>
bool DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::remove(K1 &&k) {
  auto hash = Hash();
  auto key_hash = hash(std::forward<K1>(k));
  auto shard_idx = get_shard_idx(key_hash);
  auto &shard = shards_[shard_idx];
  return shard.__run(&HashTableShard::template remove_with_hash<K1>,
                     std::forward<K1>(k), key_hash);
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
template <typename K1, typename RetT, typename... A0s, typename... A1s>
RetT DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::apply(
    K1 &&k, RetT (*fn)(std::pair<const K, V> &, A0s...), A1s &&... args) {
  auto hash = Hash();
  auto key_hash = hash(std::forward<K1>(k));
  auto shard_idx = get_shard_idx(key_hash);
  auto &shard = shards_[shard_idx];
  return shard.__run(
      +[](HashTableShard &shard, K1 &&k, uint64_t key_hash,
          RetT (*fn)(std::pair<const K, V> &, A0s...), A1s &&... args) {
        return shard.apply_with_hash(std::forward<K1>(k), key_hash, fn,
                                     std::forward<A1s>(args)...);
      },
      std::forward<K1>(k), key_hash, fn, std::forward<A1s>(args)...);
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
template <typename K1>
Future<std::optional<V>>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::get_async(K1 &&k) {
  return nu::async([&, k] { return get(std::move(k)); });
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
template <typename K1, typename V1>
Future<void>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::put_async(K1 &&k,
                                                                  V1 &&v) {
  return nu::async([&, k, v] { return put(std::move(k), std::move(v)); });
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
template <typename K1>
Future<bool>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::remove_async(K1 &&k) {
  return nu::async([&, k] { return remove(std::move(k)); });
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
template <typename K1, typename RetT, typename... A0s, typename... A1s>
Future<RetT>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::apply_async(
    K1 &&k, RetT (*fn)(std::pair<const K, V> &, A0s...), A1s &&... args) {
  return nu::async([&, k, fn, args...] {
    return apply(std::move(k), fn, std::move(args)...);
  });
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
std::vector<std::pair<K, V>>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::get_all_pairs() {
  std::vector<std::pair<K, V>> vec;
  std::vector<Future<std::vector<std::pair<K, V>>>> futures;
  for (uint32_t i = 0; i < num_shards_; i++) {
    futures.emplace_back(shards_[i].__run_async(
        +[](HashTableShard &shard) { return shard.get_all_pairs(); }));
  }
  for (auto &future : futures) {
    auto &vec_shard = future.get();
    vec.insert(vec.end(), vec_shard.begin(), vec_shard.end());
  }
  return vec;
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::Cap
DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::get_cap() const {
  Cap cap;
  for (uint32_t i = 0; i < num_shards_; i++) {
    cap.shard_caps.push_back(shards_[i].get_cap());
  }
  return cap;
}

template <typename K, typename V, typename Hash, typename KeyEqual,
          uint64_t NumBuckets>
template <typename RetT, typename... A0s, typename... A1s>
RetT DistributedHashTable<K, V, Hash, KeyEqual, NumBuckets>::associative_reduce(
    RetT init_val,
    void (*reduce_fn)(RetT &, std::pair<const K, V> &, A0s...),
    void (*merge_fn)(RetT &, RetT &, A0s...), A1s &&... args) {
  RetT reduced_val(std::move(init_val));
  std::vector<Future<RetT>> futures;

  for (uint32_t i = 0; i < num_shards_; i++) {
    futures.emplace_back(shards_[i].__run_async(
        &HashTableShard::template associative_reduce<RetT>, reduced_val,
        reduce_fn, std::forward<A1s>(args)...));
  }

  for (auto &future : futures) {
    merge_fn(reduced_val, future.get(), std::forward<A1s>(args)...);
  }

  return reduced_val;
}

} // namespace nu
