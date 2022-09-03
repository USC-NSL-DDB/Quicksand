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
  std::optional<Val> find_val(Key k);
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
  constexpr static uint32_t kBatchingMaxBatchBytes = 16 << 10;
  constexpr static uint32_t kLowLatencyMaxShardBytes = 16 << 20;
  constexpr static uint32_t kLowLatencyMaxBatchBytes = 0;

  using KeyToShardsMapping = std::map<
      std::optional<Key>,
      std::pair<WeakProclet<Shard>, std::vector<ContainerReq<Key, Val>>>>;
  struct ReqBatch {
    std::optional<Key> l_key;
    std::optional<Key> r_key;
    WeakProclet<Shard> shard;
    std::vector<ContainerReq<Key, Val>> reqs;
  };

  Proclet<ShardingMapping> mapping_;
  uint32_t max_shard_size_;
  uint32_t max_batch_size_;
  KeyToShardsMapping key_to_shards_;
  Future<std::optional<ReqBatch>> flush_future_;
  template <ShardedDataStructureBased T>
  friend class SealedDS;

  std::size_t __size();
  void reset();
  void set_shard_and_batch_size();
  bool flush_one_batch(KeyToShardsMapping::iterator iter);
  void handle_rejected_flush_batch(ReqBatch &batch);
  void sync_mapping(std::optional<Key> l_key, std::optional<Key> r_key);
  std::pair<std::optional<Key>, std::optional<Key>> get_key_range(
      KeyToShardsMapping::iterator iter);
  std::vector<WeakProclet<Shard>> get_all_non_empty_shards();
  void seal();
  void unseal();
};

}  // namespace nu

#include "nu/impl/sharded_ds.ipp"
