#include <algorithm>
#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <cstdint>

#include "nu/sharded_pair_collect.hpp"

namespace nu {

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(uint32_t shard_size)
    : mapping_(make_proclet<ShardingMapping>()), shard_size_(shard_size) {
  auto initial_shard = make_proclet<Shard>(mapping_.get_weak(), shard_size);
  mapping_.run(&ShardingMapping::set_initial_shard, initial_shard);
}

template <typename K, typename V>
template <typename K1, typename V1>
void ShardedPairCollection<K, V>::emplace_back(K1 &&k, V1 &&v) {
  auto shard = mapping_.run(&ShardingMapping::template get_shard<K>,
                            std::forward<K1>(k));
  shard.run(
      +[](Shard &shard, K k, V v) {
        shard.emplace_back(std::make_pair(std::move(k), std::move(v)));
      },
      std::forward<K1>(k), std::forward<V1>(v));
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::emplace_back(PairType &&p) {
  auto shard = mapping_.run(&ShardingMapping::template get_shard<K>, p.first);
  shard.run(
      +[](Shard &shard, PairType p) { shard.emplace_back(std::move(p)); },
      std::move(p));
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::emplace_back(const PairType &p) {
  auto shard = mapping_.run(&ShardingMapping::template get_shard<K>, p.first);
  shard.run(
      +[](Shard &shard, PairType p) { shard.emplace_back(std::move(p)); }, p);
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
ShardedPairCollection<K, V>::Shard::Shard(WeakProclet<ShardingMapping> mapping,
                                          uint32_t shard_size)
    : shard_size_(shard_size), mapping_(std::move(mapping)) {}

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
void ShardedPairCollection<K, V>::Shard::emplace_back(PairType &&p) {
  ScopedLock<SpinLock> guard(&spin_);

  data_.emplace_back(std::move(p));
  if (unlikely(data_.size() * sizeof(PairType) > shard_size_)) {
    auto new_shard_future = make_proclet_async<Shard>(mapping_, shard_size_);
    auto mid = data_.begin() + data_.size() / 2;
    std::nth_element(data_.begin(), mid, data_.end());
    auto mid_k = data_[data_.size() / 2].first;
    // TODO: get rid of copy.
    ShardDataType post_split_data(mid, data_.end());
    data_.erase(mid, data_.end());
    auto &new_shard = new_shard_future.get();
    new_shard.run(&Shard::set_data, std::move(post_split_data));
    mapping_.run(&ShardingMapping::template update_mapping<K>, std::move(mid_k),
                 std::move(new_shard));
  }
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::Shard::set_data(ShardDataType data) {
  ScopedLock<SpinLock> guard(&spin_);
  data_ = std::move(data);
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardingMapping::ShardingMapping() {}

template <typename K, typename V>
template <typename K1>
WeakProclet<typename ShardedPairCollection<K, V>::Shard>
ShardedPairCollection<K, V>::ShardingMapping::get_shard(K1 k1) {
  auto iter = mapping_.upper_bound(k1);
  auto idx = (iter == mapping_.end()) ? 0 : iter->second;
  return shards_[idx].get_weak();
}

template <typename K, typename V>
std::vector<WeakProclet<typename ShardedPairCollection<K, V>::Shard>>
ShardedPairCollection<K, V>::ShardingMapping::get_all_shards() {
  std::vector<WeakProclet<Shard>> shards;
  for (auto &shard : shards_) {
    shards.emplace_back(shard.get_weak());
  }
  return shards;
}

template <typename K, typename V>
template <typename K1>
void ShardedPairCollection<K, V>::ShardingMapping::update_mapping(
    K1 k1, Proclet<Shard> shard) {
  shards_.emplace_back(std::move(shard));
  auto ret = mapping_.try_emplace(k1, shards_.size() - 1);
  BUG_ON(!ret.second);
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::ShardingMapping::set_initial_shard(
    Proclet<Shard> shard) {
  shards_.emplace_back(std::move(shard));
}

}  // namespace nu
