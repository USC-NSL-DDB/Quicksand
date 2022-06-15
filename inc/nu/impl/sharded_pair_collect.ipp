#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <cstdint>

#include "nu/sharded_pair_collect.hpp"

namespace nu {

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(uint32_t shard_size)
    : mapping_(make_proclet<ShardingMapping>(shard_size)) {}

template <typename K, typename V>
template <typename K1, typename V1>
void ShardedPairCollection<K, V>::emplace_back(K1 &&k, V1 &&v) {
  auto shard = mapping_.run(&ShardingMapping::template get_shard<K1>,
                            std::forward<K1>(k));
  shard.run(
      +[](ShardType &vec, K k, V v) { vec.emplace_back(std::make_pair(k, v)); },
      std::forward<K1>(k), std::forward<V1>(v));
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::emplace_back(PairType &&p) {
  auto shard = mapping_.run(&ShardingMapping::template get_shard<K>, p.first);
  shard.run(
      +[](ShardType &vec, PairType p) { vec.emplace_back(std::move(p)); },
      std::move(p));
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::emplace_back(const PairType &p) {
  auto shard = mapping_.run(&ShardingMapping::template get_shard<K>, p.first);
  shard.run(
      +[](ShardType &vec, PairType p) { vec.emplace_back(std::move(p)); }, p);
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
        +[](ShardType &vec, uintptr_t raw_fn, S1s... states) {
          auto *fn = reinterpret_cast<Fn>(raw_fn);
          for (auto &t : vec) {
            fn(reinterpret_cast<std::pair<const K, V> &>(t),
               std::move(states)...);
          }
        },
        raw_fn, states...));
  }
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardType ShardedPairCollection<K, V>::collect() {
  auto shards = mapping_.run(&ShardingMapping::get_all_shards);
  std::vector<Future<ShardType>> futures;
  for (auto &shard : shards) {
    futures.emplace_back(shard.run_async(+[](ShardType &vec) { return vec; }));
  }
  ShardType all;
  for (auto &future : futures) {
    auto &vec = future.get();
    all.insert(all.end(), vec.begin(), vec.end());
  }
  return all;
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardingMapping::ShardingMapping(
    uint32_t shard_size)
    : shard_size_(shard_size) {
  shards_.emplace_back(make_proclet<ShardType>());
}

template <typename K, typename V>
template <typename K1>
WeakProclet<typename ShardedPairCollection<K, V>::ShardType>
ShardedPairCollection<K, V>::ShardingMapping::get_shard(K1 k1) {
  auto iter = mapping_.upper_bound(k1);
  auto idx = (iter == mapping_.end()) ? 0 : iter->second;
  return shards_[idx].get_weak();
}

template <typename K, typename V>
std::vector<WeakProclet<typename ShardedPairCollection<K, V>::ShardType>>
ShardedPairCollection<K, V>::ShardingMapping::get_all_shards() {
  std::vector<WeakProclet<ShardType>> shards;
  for (auto &shard : shards_) {
    shards.emplace_back(shard.get_weak());
  }
  return shards;
}

template <typename K, typename V>
template <typename K1>
void ShardedPairCollection<K, V>::ShardingMapping::update_mapping(
    K1 k1, Proclet<ShardType> proclet) {
  shards_.emplace_back(std::move(proclet));
  auto ret = mapping_.try_emplace(k1, shards_.size() - 1);
  BUG_ON(!ret->second);
}
}  // namespace nu
