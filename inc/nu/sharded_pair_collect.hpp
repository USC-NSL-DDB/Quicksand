#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <queue>
#include <utility>
#include <vector>

#include "nu/utils/future.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/reader_writer_lock.hpp"
#include "nu/utils/scoped_lock.hpp"
#include "nu/utils/span_to_vector.hpp"

namespace nu {

template <typename K, typename V>
class ShardedPairCollection {
 public:
  using PairType = std::pair<K, V>;
  using ShardDataType = std::vector<PairType>;
  constexpr static uint32_t kDefaultShardBytes = 2 << 20;
  constexpr static uint32_t kDefaultCacheBucketBytes = 16 << 10;

  ShardedPairCollection(uint32_t shard_bytes = kDefaultShardBytes,
                        uint32_t cache_bucket_bytes = kDefaultCacheBucketBytes);
  ShardedPairCollection(const ShardedPairCollection &);
  ShardedPairCollection &operator=(const ShardedPairCollection &);
  ShardedPairCollection(ShardedPairCollection &&) noexcept;
  ShardedPairCollection &operator=(ShardedPairCollection &&) noexcept;
  template <typename K1, typename V1>
  void emplace(K1 &&k1, V1 &&v1);
  void emplace(PairType &&p);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(std::pair<const K, V> &, S0s...), S1s &&... states);
  ShardDataType collect();
  void flush();
  template <class Archive>
  void serialize(Archive &ar);

 private:
  class ShardingMapping;
  class Shard {
   public:
    Shard(WeakProclet<ShardingMapping> mapping, uint32_t shard_size,
          std::optional<K> key_l, std::optional<K> key_r, ShardDataType data);
    Shard(WeakProclet<ShardingMapping> mapping, uint32_t shard_size,
          std::optional<K> key_l, std::optional<K> key_r,
          SpanToVectorWrapper<PairType> data);
    ShardDataType get_data();
    std::pair<ScopedLock<Mutex>, ShardDataType *> get_data_ptr();
    ShardDataType try_emplace_back(ShardDataType p);

   private:
    Mutex mutex_;
    uint32_t shard_size_;
    WeakProclet<ShardingMapping> mapping_;
    std::optional<K> key_l_;
    std::optional<K> key_r_;
    ShardDataType data_;
  };

  class ShardingMapping {
   public:
    ShardingMapping();
    std::vector<std::pair<std::optional<K>, WeakProclet<Shard>>>
    get_shards_in_range(std::optional<K> key_l, std::optional<K> key_r);
    std::vector<WeakProclet<Shard>> get_all_shards();
    template <typename K1>
    void update_mapping(K1 k, Proclet<Shard> shard);
    void set_initial_shard(Proclet<Shard> shard);

   private:
    std::map<std::optional<K>, Proclet<Shard>, std::greater<std::optional<K>>>
        mapping_;
    ReaderWriterLock rw_lock_;
  };

  struct Cache {
    WeakProclet<Shard> shard;
    ShardDataType data[kNumCores];

    template <class Archive>
    void serialize(Archive &ar);
  };

  using CacheMappingType =
      std::map<std::optional<K>, Cache, std::greater<std::optional<K>>>;

  constexpr static uint32_t kMaxNumAsyncPushDataPerCore = 32;
  Proclet<ShardingMapping> mapping_;
  ReaderWriterLock rw_lock_;
  CacheMappingType cache_mapping_;
  uint32_t shard_size_;
  uint32_t cache_bucket_size_;
  std::queue<Future<bool>> push_data_futures_[kNumCores];

  bool push_data(std::optional<K> &key_l, std::optional<K> &key_r,
                 WeakProclet<Shard> &shard, const ShardDataType &data);
  Future<bool> push_data_async(std::optional<K> key_l, std::optional<K> key_r,
                               WeakProclet<Shard> shard, ShardDataType data);
};

}  // namespace nu

#include "nu/impl/sharded_pair_collect.ipp"
