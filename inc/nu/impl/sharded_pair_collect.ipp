#include <sync.h>

#include <algorithm>
#include <cereal/types/map.hpp>
#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <cstdint>

#include "nu/sharded_pair_collect.hpp"

namespace nu {

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(
    uint32_t shard_size, uint32_t cache_mapping_bucket_size)
    : mapping_(make_proclet<ShardingMapping>()),
      shard_size_(shard_size),
      cache_bucket_size_(cache_mapping_bucket_size) {
  auto initial_shard =
      make_proclet<Shard>(mapping_.get_weak(), shard_size, std::optional<K>(),
                          std::optional<K>(), ShardDataType());
  cache_mapping_.try_emplace(std::nullopt, Cache{initial_shard.get_weak()});
  mapping_.run(&ShardingMapping::set_initial_shard, std::move(initial_shard));
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(
    const ShardedPairCollection &o)
    : mapping_(o.mapping_), shard_size_(o.shard_size_) {
  for (auto &[k, c] : o.cache_mapping_) {
    cache_mapping_.try_emplace(k, Cache{c.shard});
  }
}

template <typename K, typename V>
ShardedPairCollection<K, V> &ShardedPairCollection<K, V>::operator=(
    const ShardedPairCollection &o) {
  mapping_ = o.mapping_;
  for (auto &[k, p] : o.cache_mapping_) {
    cache_mapping_.try_emplace(k, Cache{p.first});
  }
  shard_size_ = o.shard_size_;
  return *this;
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(ShardedPairCollection &&o)
    : mapping_(std::move(o.mapping_)),
      cache_mapping_(std::move(o.cache_mapping_)),
      shard_size_(o.shard_size_) {}

template <typename K, typename V>
ShardedPairCollection<K, V> &ShardedPairCollection<K, V>::operator=(
    ShardedPairCollection &&o) {
  mapping_ = std::move(o.mapping_);
  cache_mapping_ = std::move(o.cache_mapping_);
  shard_size_ = o.shard_size_;
  return *this;
}

template <typename K, typename V>
template <typename K1, typename V1>
void ShardedPairCollection<K, V>::emplace_back(K1 &&k, V1 &&v) {
  std::pair<K, V> p(std::forward<K1>(k), std::forward<V1>(v));
  emplace_back(std::move(p));
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::emplace_back(PairType &&p) {
  rw_lock_.reader_lock();
  auto iter = cache_mapping_.lower_bound(p.first);
  BUG_ON(iter == cache_mapping_.end());

  WeakProclet<Shard> shard;
  ShardDataType data_to_push;
  {
    rt::Preempt pt;
    rt::PreemptGuard g(&pt);
    shard = iter->second.shard;
    auto &data = iter->second.data[pt.get_cpu()];

    data.emplace_back(p);
    if (unlikely(data.size() * sizeof(PairType) > cache_bucket_size_)) {
      data_to_push = std::move(data);
      data.clear();
    }
  }
  rw_lock_.reader_unlock();

  if (unlikely(!data_to_push.empty())) {
    push(shard, std::move(data_to_push));
  }
}

template <typename K, typename V>
bool ShardedPairCollection<K, V>::push(WeakProclet<Shard> shard,
                                       ShardDataType &&data_to_push,
                                       bool with_wlock) {
  auto rejected_data =
      shard.run(&Shard::try_emplace_back, std::move(data_to_push));

  if (unlikely(!rejected_data.empty())) {
    std::map<std::optional<K>, WeakProclet<Shard>> new_mappings;
    for (auto &[k, _] : rejected_data) {
      auto new_mapping =
          mapping_.run(&ShardingMapping::template get_shard<K>, k);
      new_mappings.insert(new_mapping);
    }

    if (!with_wlock) rw_lock_.writer_lock();
    for (auto &[k, s] : new_mappings) {
      cache_mapping_.try_emplace(std::move(k), Cache{s});
    }
    if (!with_wlock) rw_lock_.writer_unlock();

    rt::Preempt pt;
    rt::PreemptGuard g(&pt);
    if (!with_wlock) rw_lock_.reader_lock();
    for (auto &p : rejected_data) {
      auto iter = cache_mapping_.lower_bound(p.first);
      BUG_ON(iter == cache_mapping_.end());
      auto &data = iter->second.data[pt.get_cpu()];
      data.emplace_back(p);
    }
    if (!with_wlock) rw_lock_.reader_unlock();
    return false;
  }

  return true;
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::flush() {
  bool done = true;
  rw_lock_.writer_lock();
  do {
    for (auto &[k, cache] : cache_mapping_) {
      for (uint32_t i = 0; i < kNumCores; i++) {
        done &= push(cache.shard, std::move(cache.data[i]), true);
        cache.data[i].clear();
      }
    }
  } while (!done);
  rw_lock_.writer_unlock();
}

template <typename K, typename V>
template <typename... S0s, typename... S1s>
void ShardedPairCollection<K, V>::for_all(void (*fn)(std::pair<const K, V> &p,
                                                     S0s...),
                                          S1s &&... states) {
  flush();

  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);
  auto shards = mapping_.run(&ShardingMapping::get_all_shards);
  std::vector<Future<void>> futures;
  for (auto &shard : shards) {
    futures.emplace_back(shard.run_async(
        +[](Shard &shard, uintptr_t raw_fn, S1s... states) {
          auto *fn = reinterpret_cast<Fn>(raw_fn);
          auto pair = shard.get_data_ptr();
          for (auto &t : *pair.second) {
            fn(reinterpret_cast<std::pair<const K, V> &>(t),
               std::move(states)...);
          }
        },
        raw_fn, states...));
  }
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardDataType
ShardedPairCollection<K, V>::collect() {
  flush();

  auto shards = mapping_.run(&ShardingMapping::get_all_shards);
  std::vector<Future<ShardDataType>> futures;
  for (auto &shard : shards) {
    futures.emplace_back(shard.run_async(&Shard::get_data));
  }
  ShardDataType all;
  for (auto &future : futures) {
    auto &vec = future.get();
    all.insert(all.end(), vec.begin(), vec.end());
  }
  return all;
}

template <typename K, typename V>
template <class Archive>
void ShardedPairCollection<K, V>::serialize(Archive &ar) {
  ar(mapping_, cache_mapping_, shard_size_);
}

template <typename K, typename V>
ShardedPairCollection<K, V>::Shard::Shard(WeakProclet<ShardingMapping> mapping,
                                          uint32_t shard_size,
                                          std::optional<K> key_l,
                                          std::optional<K> key_r,
                                          ShardDataType data)
    : shard_size_(shard_size),
      mapping_(std::move(mapping)),
      key_l_(key_l),
      key_r_(key_r),
      data_(data) {}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardDataType
ShardedPairCollection<K, V>::Shard::get_data() {
  ScopedLock<SpinLock> guard(&spin_);
  return data_;
}

template <typename K, typename V>
std::pair<ScopedLock<SpinLock>,
          typename ShardedPairCollection<K, V>::ShardDataType *>
ShardedPairCollection<K, V>::Shard::get_data_ptr() {
  return std::make_pair(ScopedLock(&spin_), &data_);
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardDataType
ShardedPairCollection<K, V>::Shard::try_emplace_back(ShardDataType data) {
  ScopedLock<SpinLock> guard(&spin_);

  ShardDataType rejected_data;
  for (auto &p : data) {
    if (unlikely((key_l_ && p.first < *key_l_) ||
                 (key_r_ && p.first >= *key_r_))) {
      rejected_data.emplace_back(std::move(p));
    } else {
      data_.emplace_back(std::move(p));
    }
  }

  if (unlikely(data_.size() * sizeof(PairType) > shard_size_)) {
    auto mid = data_.begin() + data_.size() / 2;
    std::nth_element(data_.begin(), mid, data_.end());
    auto mid_k = data_[data_.size() / 2].first;
    // TODO: get rid of copy.
    ShardDataType post_split_data(mid, data_.end());
    data_.erase(mid, data_.end());
    auto new_shard = make_proclet<Shard>(mapping_, shard_size_, mid_k, key_r_,
                                         std::move(post_split_data));
    key_r_ = mid_k;
    mapping_.run(&ShardingMapping::template update_mapping<K>, std::move(mid_k),
                 std::move(new_shard));
  }

  return rejected_data;
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardingMapping::ShardingMapping() {}

template <typename K, typename V>
template <typename K1>
std::pair<std::optional<K>,
          WeakProclet<typename ShardedPairCollection<K, V>::Shard>>
ShardedPairCollection<K, V>::ShardingMapping::get_shard(K1 k1) {
  rw_lock_.reader_lock();
  auto iter = mapping_.lower_bound(k1);
  BUG_ON(iter == mapping_.end());
  auto ret = std::make_pair(iter->first, iter->second.get_weak());
  rw_lock_.reader_unlock();
  return ret;
}

template <typename K, typename V>
std::vector<WeakProclet<typename ShardedPairCollection<K, V>::Shard>>
ShardedPairCollection<K, V>::ShardingMapping::get_all_shards() {
  std::vector<WeakProclet<Shard>> shards;

  rw_lock_.reader_lock();
  for (auto &[_, shard] : mapping_) {
    shards.emplace_back(shard.get_weak());
  }
  rw_lock_.reader_unlock();
  return shards;
}

template <typename K, typename V>
template <typename K1>
void ShardedPairCollection<K, V>::ShardingMapping::update_mapping(
    K1 k1, Proclet<Shard> shard) {
  rw_lock_.writer_lock();
  auto ret = mapping_.try_emplace(k1, std::move(shard));
  rw_lock_.writer_unlock();
  BUG_ON(!ret.second);
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::ShardingMapping::set_initial_shard(
    Proclet<Shard> shard) {
  auto ret = mapping_.try_emplace(std::nullopt, std::move(shard));
  BUG_ON(!ret.second);
}

template <typename K, typename V>
template <class Archive>
void ShardedPairCollection<K, V>::Cache::serialize(Archive &ar) {
  ar(shard, data);
}

}  // namespace nu
