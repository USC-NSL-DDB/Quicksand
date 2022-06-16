#include <algorithm>
#include <cereal/types/map.hpp>
#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <cstdint>

#include "nu/sharded_pair_collect.hpp"

namespace nu {

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(uint32_t shard_size)
    : mapping_(make_proclet<ShardingMapping>()), shard_size_(shard_size) {
  auto initial_shard =
      make_proclet<Shard>(mapping_.get_weak(), shard_size, std::optional<K>(),
                          std::optional<K>(), ShardDataType());
  cached_mapping_.try_emplace(std::nullopt, initial_shard.get_weak());
  mapping_.run(&ShardingMapping::set_initial_shard, std::move(initial_shard));
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(
    const ShardedPairCollection &o)
    : mapping_(o.mapping_),
      cached_mapping_(o.cached_mapping_),
      shard_size_(o.shard_size_) {}

template <typename K, typename V>
ShardedPairCollection<K, V> &ShardedPairCollection<K, V>::operator=(
    const ShardedPairCollection &o) {
  this->mapping_ = o.mapping_;
  this->cached_mapping_ = o.cached_mapping_;
  this->shard_size_ = o.shard_size_;
  return *this;
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(ShardedPairCollection &&o)
    : mapping_(std::move(o.mapping_)),
      cached_mapping_(std::move(o.cached_mapping_)),
      shard_size_(o.shard_size_) {}

template <typename K, typename V>
ShardedPairCollection<K, V> &ShardedPairCollection<K, V>::operator=(
    ShardedPairCollection &&o) {
  this->mapping_ = std::move(o.mapping_);
  this->cached_mapping_ = std::move(o.cached_mapping_);
  this->shard_size_ = o.shard_size_;
  return *this;
}

template <typename K, typename V>
template <typename K1, typename V1>
void ShardedPairCollection<K, V>::emplace_back(K1 &&k, V1 &&v) {
  std::pair<K, V> p(std::forward<K1>(k), std::forward<V1>(v));
  emplace_back(p);
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::emplace_back(const PairType &p) {
  auto shard = get_shard(p.first);
retry:
  auto success = shard.run(
      +[](Shard &shard, PairType p) {
        return shard.emplace_back(std::move(p));
      },
      p);
  if (unlikely(!success)) {
    shard = get_shard(p.first, /* invalidate_cache = */ true);
    goto retry;
  }
}

template <typename K, typename V>
template <typename... S0s, typename... S1s>
void ShardedPairCollection<K, V>::for_all(void (*fn)(std::pair<const K, V> &p,
                                                     S0s...),
                                          S1s &&... states) {
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
  ar(mapping_, cached_mapping_, shard_size_);
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
bool ShardedPairCollection<K, V>::Shard::emplace_back(PairType &&p) {
  ScopedLock<SpinLock> guard(&spin_);

  if ((key_l_ && p.first < *key_l_) || (key_r_ && p.first >= *key_r_)) {
    return false;
  }

  data_.emplace_back(std::move(p));
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
  return true;
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardingMapping::ShardingMapping() {}

template <typename K, typename V>
template <typename K1>
std::pair<std::optional<K>,
          WeakProclet<typename ShardedPairCollection<K, V>::Shard>>
ShardedPairCollection<K, V>::ShardingMapping::get_shard(K1 k1) {
  auto iter = mapping_.lower_bound(k1);
  BUG_ON(iter == mapping_.end());
  return std::make_pair(iter->first, iter->second.get_weak());
}

template <typename K, typename V>
std::vector<WeakProclet<typename ShardedPairCollection<K, V>::Shard>>
ShardedPairCollection<K, V>::ShardingMapping::get_all_shards() {
  std::vector<WeakProclet<Shard>> shards;
  for (auto &[_, shard] : mapping_) {
    shards.emplace_back(shard.get_weak());
  }
  return shards;
}

template <typename K, typename V>
template <typename K1>
void ShardedPairCollection<K, V>::ShardingMapping::update_mapping(
    K1 k1, Proclet<Shard> shard) {
  auto ret = mapping_.try_emplace(k1, std::move(shard));
  BUG_ON(!ret.second);
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::ShardingMapping::set_initial_shard(
    Proclet<Shard> shard) {
  auto ret = mapping_.try_emplace(std::nullopt, std::move(shard));
  BUG_ON(!ret.second);
}

template <typename K, typename V>
template <typename K1>
WeakProclet<typename ShardedPairCollection<K, V>::Shard>
ShardedPairCollection<K, V>::get_shard(const K1 &k1, bool invalidate_cache) {
  ScopedLock<SpinLock> guard(&spin_);

  if (unlikely(invalidate_cache)) {
    auto pair = mapping_.run(&ShardingMapping::template get_shard<K1>, k1);
    cached_mapping_.insert(std::move(pair));
  }
  auto iter = cached_mapping_.lower_bound(k1);
  BUG_ON(iter == cached_mapping_.end());
  return iter->second;
}

}  // namespace nu
