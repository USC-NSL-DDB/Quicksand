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
    uint32_t shard_bytes, uint32_t cache_mapping_bucket_bytes)
    : mapping_(make_proclet<ShardingMapping>()),
      shard_size_(shard_bytes / sizeof(PairType)),
      cache_bucket_size_(cache_mapping_bucket_bytes / sizeof(PairType)) {
  auto initial_shard =
      make_proclet<Shard>(mapping_.get_weak(), shard_size_, std::optional<K>(),
                          std::optional<K>(), ShardDataType());
  cache_mapping_.try_emplace(std::nullopt, Cache{initial_shard.get_weak()});
  mapping_.run(&ShardingMapping::set_initial_shard, initial_shard);
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn, uint32_t shard_bytes,
    uint32_t cache_mapping_bucket_bytes)
    : ShardedPairCollection(shard_bytes, cache_mapping_bucket_bytes) {
  auto num_shards = (num - 1) / shard_size_ + 1;
  std::vector<Future<Proclet<Shard>>> shard_futures;

  auto k = estimated_min_key;
  for (uint32_t i = 0; i < num_shards; i++) {
    auto prev_k = k;
    key_inc_fn(k, shard_size_);
    shard_futures.emplace_back(make_proclet_async<Shard>(
        mapping_.get_weak(), shard_size_, prev_k, k, ShardDataType()));
  }

  k = estimated_min_key;
  for (auto &shard_future : shard_futures) {
    auto &shard = shard_future.get();
    auto weak_shard = shard.get_weak();
    auto update_future = mapping_.run_async(
        &ShardingMapping::template update_mapping<K>, k, std::move(shard));
    cache_mapping_.try_emplace(k, Cache{weak_shard});
    key_inc_fn(k, shard_size_);
  }
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
ShardedPairCollection<K, V>::ShardedPairCollection(
    ShardedPairCollection &&o) noexcept
    : mapping_(std::move(o.mapping_)),
      cache_mapping_(std::move(o.cache_mapping_)),
      shard_size_(o.shard_size_) {
  for (uint32_t i = 0; i < kNumCores; i++) {
    push_data_futures_[i] = std::move(o.push_data_futures_[i]);
  }
}

template <typename K, typename V>
ShardedPairCollection<K, V> &ShardedPairCollection<K, V>::operator=(
    ShardedPairCollection &&o) noexcept {
  mapping_ = std::move(o.mapping_);
  cache_mapping_ = std::move(o.cache_mapping_);
  shard_size_ = o.shard_size_;
  for (uint32_t i = 0; i < kNumCores; i++) {
    push_data_futures_[i] = std::move(o.push_data_futures_[i]);
  }
  return *this;
}

template <typename K, typename V>
template <typename K1, typename V1>
void ShardedPairCollection<K, V>::emplace(K1 &&k, V1 &&v) {
  emplace({std::forward<K1>(k), std::forward<V1>(v)});
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::emplace(PairType &&p) {
  rw_lock_.reader_lock_np();
  auto iter = cache_mapping_.lower_bound(p.first);
  assert(iter != cache_mapping_.end());

  auto core_id = read_cpu();
  auto &data = iter->second.data[core_id];
  data.emplace_back(std::move(p));

  if (unlikely(data.size() > cache_bucket_size_)) {
    auto &shard = iter->second.shard;
    auto &l_key = iter->first;
    std::optional<K> r_key;
    if (iter != cache_mapping_.begin()) {
      r_key = (--iter)->first;
    }

    auto &futures = push_data_futures_[core_id];
    Future<bool> oldest_future;
    if (futures.size() == kMaxNumAsyncPushDataPerCore) {
      oldest_future = std::move(futures.front());
      futures.pop();
    }
    futures.emplace(
        push_data_async(l_key, std::move(r_key), shard, std::move(data)));
    data.clear();
    rw_lock_.reader_unlock_np();
    return;
  }

  rw_lock_.reader_unlock_np();
}

template <typename K, typename V>
Future<bool> ShardedPairCollection<K, V>::push_data_async(
    std::optional<K> l_key, std::optional<K> r_key, WeakProclet<Shard> shard,
    ShardDataType data) {
  return nu::async([this, l_key = std::move(l_key), r_key = std::move(r_key),
                    shard = std::move(shard),
                    data = std::move(data)]() mutable {
    return push_data(l_key, r_key, shard, data);
  });
}

template <typename K, typename V>
bool ShardedPairCollection<K, V>::push_data(std::optional<K> &l_key,
                                            std::optional<K> &r_key,
                                            WeakProclet<Shard> &shard,
                                            const ShardDataType &data) {
  auto rejected_data = shard.run(&Shard::try_emplace_back, data);

  if (unlikely(!rejected_data.empty())) {
    auto new_mappings =
        mapping_.run(&ShardingMapping::get_shards_in_range, l_key, r_key);

    rw_lock_.writer_lock();
    for (auto &[k, s] : new_mappings) {
      cache_mapping_.try_emplace(std::move(k), Cache{s});
    }
    rw_lock_.writer_unlock();

    rw_lock_.reader_lock_np();
    for (auto &p : rejected_data) {
      auto iter = cache_mapping_.lower_bound(p.first);
      BUG_ON(iter == cache_mapping_.end());
      auto &data = iter->second.data[read_cpu()];
      data.emplace_back(p);
    }
    rw_lock_.reader_unlock_np();
    return false;
  }

  return true;
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::flush() {
  std::vector<Future<bool>> futures;

  rw_lock_.writer_lock();

  for (auto &q : push_data_futures_) {
    while (!q.empty()) {
      futures.emplace_back(std::move(q.front()));
      q.pop();
    }
  }

again:
  std::optional<K> r_key;
  for (auto iter = cache_mapping_.begin(); iter != cache_mapping_.end();
       iter++) {
    auto &[l_key, cache] = *iter;
    for (uint32_t i = 0; i < kNumCores; i++) {
      auto &data = cache.data[i];
      if (!data.empty()) {
        futures.emplace_back(
            push_data_async(l_key, r_key, cache.shard, std::move(data)));
        data.clear();
      }
    }
    r_key = l_key;
  }

  rw_lock_.writer_unlock();

  bool done = true;
  for (auto &future : futures) {
    done &= future.get();
  }

  if (unlikely(!done)) {
    futures.clear();
    rw_lock_.writer_lock();
    goto again;
  }
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
                                          std::optional<K> l_key,
                                          std::optional<K> r_key,
                                          ShardDataType data)
    : shard_size_(shard_size),
      mapping_(std::move(mapping)),
      l_key_(l_key),
      r_key_(r_key),
      data_(data) {}

template <typename K, typename V>
ShardedPairCollection<K, V>::Shard::Shard(WeakProclet<ShardingMapping> mapping,
                                          uint32_t shard_size,
                                          std::optional<K> l_key,
                                          std::optional<K> r_key,
                                          SpanToVectorWrapper<PairType> data)
    : shard_size_(shard_size),
      mapping_(std::move(mapping)),
      l_key_(l_key),
      r_key_(r_key),
      data_(std::move(data.unwrap())) {}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardDataType
ShardedPairCollection<K, V>::Shard::get_data() {
  ScopedLock<Mutex> guard(&mutex_);
  return data_;
}

template <typename K, typename V>
std::pair<ScopedLock<Mutex>,
          typename ShardedPairCollection<K, V>::ShardDataType *>
ShardedPairCollection<K, V>::Shard::get_data_ptr() {
  return std::make_pair(&mutex_, &data_);
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardDataType
ShardedPairCollection<K, V>::Shard::try_emplace_back(ShardDataType data) {
  ScopedLock<Mutex> guard(&mutex_);

  ShardDataType rejected_data;
  for (auto &p : data) {
    if (unlikely((l_key_ && p.first < *l_key_) ||
                 (r_key_ && p.first >= *r_key_))) {
      rejected_data.emplace_back(std::move(p));
    } else {
      data_.emplace_back(std::move(p));
    }
  }

  if (unlikely(data_.size() > shard_size_)) {
    auto mid = data_.begin() + data_.size() / 2;
    std::nth_element(data_.begin(), mid, data_.end());
    auto mid_k = data_[data_.size() / 2].first;
    SpanToVectorWrapper post_split_data(std::span(mid, data_.end()));
    auto new_shard = make_proclet<Shard>(mapping_, shard_size_, mid_k, r_key_,
                                         post_split_data);
    data_.erase(mid, data_.end());
    r_key_ = mid_k;
    mapping_.run(&ShardingMapping::template update_mapping<K>, mid_k,
                 std::move(new_shard));
  }

  return rejected_data;
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardingMapping::ShardingMapping() {}

template <typename K, typename V>
std::vector<std::pair<std::optional<K>,
                      WeakProclet<typename ShardedPairCollection<K, V>::Shard>>>
ShardedPairCollection<K, V>::ShardingMapping::get_shards_in_range(
    std::optional<K> l_key, std::optional<K> r_key) {
  std::vector<std::pair<std::optional<K>, WeakProclet<Shard>>> shards;
  rw_lock_.reader_lock();
  typename decltype(mapping_)::iterator iter =
      r_key ? mapping_.upper_bound(r_key) : mapping_.begin();
  while (iter != mapping_.end() && iter->first >= l_key) {
    shards.emplace_back(iter->first, iter->second.get_weak());
    iter++;
  }
  auto ret = std::make_pair(iter->first, iter->second.get_weak());
  rw_lock_.reader_unlock();
  return shards;
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
    K1 k, Proclet<Shard> shard) {
  rw_lock_.writer_lock();
  auto ret = mapping_.try_emplace(k, std::move(shard));
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
