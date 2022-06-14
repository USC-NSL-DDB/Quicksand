#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <cstdint>

#include "nu/sharded_pair_collect.hpp"

namespace nu {

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(uint32_t shard_size)
    : shard_size_(shard_size), shard_(make_proclet<ShardType>()) {}

template <typename K, typename V>
template <typename K1, typename V1>
void ShardedPairCollection<K, V>::emplace_back(K1 &&k, V1 &&v) {
  shard_.run(
      +[](ShardType &vec, K k, V v) { vec.emplace_back(std::make_pair(k, v)); },
      std::forward<K1>(k), std::forward<V1>(v));
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::emplace_back(PairType &&p) {
  shard_.run(
      +[](ShardType &vec, PairType p) { vec.emplace_back(std::move(p)); },
      std::move(p));
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::emplace_back(const PairType &p) {
  shard_.run(
      +[](ShardType &vec, PairType p) { vec.emplace_back(std::move(p)); }, p);
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::emplace_back_batch(const ShardType &vec) {
  shard_.run(
      +[](ShardType &vec, ShardType new_vec) {
        vec.insert(vec.end(), new_vec.begin(), new_vec.end());
      },
      vec);
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::emplace_back_batch(ShardType &&vec) {
  shard_.run(
      +[](ShardType &vec, ShardType new_vec) {
        vec.insert(vec.end(), new_vec.begin(), new_vec.end());
      },
      std::move(vec));
}

template <typename K, typename V>
template <typename... S0s, typename... S1s>
void ShardedPairCollection<K, V>::for_all(void (*fn)(std::pair<const K, V> &p,
                                                     S0s...),
                                          S1s &&... states) {
  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);
  shard_.run(
      +[](ShardType &vec, uintptr_t raw_fn, S1s... states) {
        auto *fn = reinterpret_cast<Fn>(raw_fn);
        for (auto &t : vec) {
          fn(reinterpret_cast<std::pair<const K, V> &>(t),
             std::move(states)...);
        }
      },
      raw_fn, std::forward<S1s>(states)...);
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardType ShardedPairCollection<K, V>::collect() {
  return shard_.run(+[](ShardType &vec) { return vec; });
}

}  // namespace nu
