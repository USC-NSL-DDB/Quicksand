#pragma once

#include <concepts>
#include <functional>
#include <list>
#include <map>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "nu/commons.hpp"
#include "nu/container.hpp"
#include "nu/proclet.hpp"
#include "nu/rem_unique_ptr.hpp"
#include "nu/shard.hpp"
#include "nu/type_traits.hpp"
#include "nu/utils/future.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/reader_writer_lock.hpp"
#include "nu/utils/scoped_lock.hpp"

namespace nu {

template <GeneralContainerBased Container, BoolIntegral LL>
class ShardedDataStructure;

template <class T>
concept ShardedDataStructureBased = requires {
  requires is_base_of_template_v<T, ShardedDataStructure>;
};

template <GeneralContainerBased Container, BoolIntegral LL>
class ShardedDataStructure {
 public:
  using Key = Container::Key;
  using Val = Container::Val;
  using IterVal = Container::IterVal;
  using Pair = Container::Pair;
  using Shard = GeneralShard<Container>;
  using ShardingMapping = GeneralShardingMapping<Shard>;

  struct Hint {
    uint64_t num;
    Key estimated_min_key;
    std::function<void(Key &, uint64_t)> key_inc_fn;
  };

  void emplace(Key k, Val v);
  void emplace(Pair p);
  void emplace_back(Val v) requires EmplaceBackAble<Container>;
  std::optional<IterVal> find_val(Key k) const requires Findable<Container>;
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  Container collect();
  bool empty() const;
  std::size_t size() const;
  void clear();
  void flush();
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 protected:
  ShardedDataStructure();
  ShardedDataStructure(std::optional<Hint> hint);
  ShardedDataStructure(const ShardedDataStructure &);
  ShardedDataStructure &operator=(const ShardedDataStructure &);
  ShardedDataStructure(ShardedDataStructure &&) noexcept;
  ShardedDataStructure &operator=(ShardedDataStructure &&) noexcept;
  ~ShardedDataStructure();

 private:
  constexpr static uint32_t kBatchingMaxShardBytes = 128 << 20;
  constexpr static uint32_t kBatchingMaxBatchBytes = 64 << 10;
  constexpr static uint32_t kLowLatencyMaxShardBytes = 16 << 20;
  constexpr static uint32_t kLowLatencyMaxBatchBytes = 0;
  constexpr static uint32_t kMaxNumInflightFlushes = 2;

  using ReqBatch = Shard::ReqBatch;
  struct ShardAndReqs {
    WeakProclet<Shard> shard;
    uint32_t seq;
    RemUniquePtr<RobExecutor<ReqBatch, std::optional<ReqBatch>>> flush_executor;
    std::vector<std::pair<Key, Val>> emplace_reqs;

    ShardAndReqs() = default;
    ShardAndReqs(WeakProclet<Shard> s);
    ShardAndReqs(const ShardAndReqs &);
    ShardAndReqs(ShardAndReqs &&) = default;

    template <class Archive>
    void save(Archive &ar) const;
    template <class Archive>
    void load(Archive &ar);
  };
  using KeyToShardsMapping = std::multimap<std::optional<Key>, ShardAndReqs>;

  Proclet<ShardingMapping> mapping_;
  KeyToShardsMapping key_to_shards_;
  std::vector<Val> emplace_back_reqs_;
  std::queue<Future<std::optional<typename Shard::ReqBatch>>> flush_futures_;
  template <ShardedDataStructureBased T>
  friend class SealedDS;

  std::size_t __size();
  std::optional<IterVal> __find_val(Key k) requires Findable<Container>;
  void reset();
  void set_shard_and_batch_size(uint32_t *max_shard_bytes,
                                uint32_t *max_shard_size,
                                uint32_t *max_batch_size);
  bool flush_one_batch(KeyToShardsMapping::iterator iter, bool drain);
  void handle_rejected_flush_batch(ReqBatch &batch);
  void sync_mapping(std::optional<Key> l_key, std::optional<Key> r_key,
                    WeakProclet<Shard> shard);
  void flush_and_sync_mapping();
  std::pair<std::optional<Key>, std::optional<Key>> get_key_range(
      KeyToShardsMapping::iterator iter);
  std::pair<std::vector<std::optional<Key>>, std::vector<WeakProclet<Shard>>>
  get_all_non_empty_shards();
  void seal();
  void unseal();
};

}  // namespace nu

#include "nu/impl/sharded_ds.ipp"
