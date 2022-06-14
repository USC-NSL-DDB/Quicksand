#pragma once

#include <cstdint>
#include <vector>

namespace nu {

template <typename T>
class ShardedCollection {
 public:
  constexpr static uint32_t kDefaultShardSize = 2 << 20;

  ShardedCollection(uint32_t shard_size = kDefaultShardSize);
  template <typename U>
  void emplace_back(U &&u);
  void emplace_back_batch(const std::vector<T> &v);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(T &, S0s...), S1s &&... states);
  std::vector<T> collect();

 private:
  uint32_t shard_size_;
  Proclet<std::vector<T>> shard_;
};

}  // namespace nu

#include "nu/impl/sharded_collection.ipp"
