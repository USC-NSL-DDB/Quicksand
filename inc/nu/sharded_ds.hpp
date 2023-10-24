#pragma once

#include <concepts>
#include <deque>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <unordered_map>
#include <unordered_set>
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
#include "nu/utils/scoped_lock.hpp"

namespace nu {

template <GeneralContainerBased Container, BoolIntegral LL>
class ShardedDataStructure;

template <class T>
concept ShardedDataStructureBased =
    requires { requires is_base_of_template_v<T, ShardedDataStructure>; };

template <GeneralContainerBased Container, BoolIntegral LL>
class ShardedDataStructure {
 public:
  using Key = Container::Key;
  using Val = Container::Val;
  using IterVal = Container::IterVal;
  using DataEntry = Container::DataEntry;
  using ContainerImpl = Container::Implementation;
  using Shard = GeneralShard<Container>;
  using ShardMapping = GeneralShardMapping<Shard>;

  constexpr static uint32_t kBatchingMaxShardBytes = 32 << 20;
  constexpr static uint32_t kBatchingMaxBatchBytes = 64 << 10;
  constexpr static uint32_t kLowLatencyMaxShardBytes = 16 << 20;
  constexpr static uint32_t kLowLatencyMaxBatchBytes = 0;
  constexpr static uint32_t kMaxNumInflightFlushes = 8;
  constexpr static uint32_t kFlushFutureRecheckUs = 10;

  struct ShardingHint {
    uint64_t num;
    Key estimated_min_key;
    std::function<void(Key &, uint64_t)> key_inc_fn;
  };

  template <typename K, typename V>
  void insert(K &&k, V &&v)
    requires(HasVal<Container> && InsertAble<Container>);
  void insert(const DataEntry &entry)
    requires InsertAble<Container>;
  void insert(DataEntry &&entry)
    requires InsertAble<Container>;
  bool erase(const Key &k)
    requires EraseAble<Container>;
  bool erase(Key &&k)
    requires EraseAble<Container>;
  void push_front(const Val &v)
    requires PushFrontAble<Container>;
  void push_front(Val &&v)
    requires PushFrontAble<Container>;
  void push_back(const Val &v)
    requires PushBackAble<Container>;
  void push_back(Val &&v)
    requires PushBackAble<Container>;
  Val front() const
    requires HasFront<Container>;
  Val pop_front()
    requires TryPopFrontAble<Container>;
  std::vector<Val> try_pop_front(std::size_t elems)
    requires TryPopFrontAble<Container>;
  Val back() const
    requires HasBack<Container>;
  Val pop_back()
    requires TryPopBackAble<Container>;
  std::vector<Val> try_pop_back(std::size_t elems)
    requires TryPopBackAble<Container>;
  std::optional<IterVal> find_data(Key k) const
    requires FindDataAble<Container>;
  void concat(ShardedDataStructure &&tail)
    requires Container::kContiguousIterator;
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&...states)
    requires HasVal<Container>;
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, S0s...), S1s &&...states)
    requires(!HasVal<Container>);
  template <typename... S0s, typename... S1s>
  void for_all_shards(void (*fn)(ContainerImpl &container_impl, S0s...),
                      S1s &&...states);
  Container collect();
  bool empty() const;
  std::size_t size() const;
  void clear()
    requires ClearAble<Container>;
  void flush();
  template <typename RetT, typename... S0s, typename... S1s>
  RetT compute_on(Key k, RetT (*fn)(ContainerImpl &container, S0s...),
                  S1s &&...states);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT run(Key k, RetT (*fn)(ContainerImpl &container, Key k, S0s...),
           S1s &&...states);
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void save_move(Archive &ar);
  template <class Archive>
  void load(Archive &ar);

 protected:
  ShardedDataStructure();
  template <typename... As>
  ShardedDataStructure(std::optional<ShardingHint> sharding_hint,
                       std::optional<NodeIP> pinned_ip, As &&...args);
  ShardedDataStructure(const ShardedDataStructure &);
  ShardedDataStructure &operator=(const ShardedDataStructure &);
  ShardedDataStructure(ShardedDataStructure &&) noexcept;
  ShardedDataStructure &operator=(ShardedDataStructure &&) noexcept;
  ~ShardedDataStructure();

 private:
  using ReqBatch = Shard::ReqBatch;
  struct ShardAndReqs {
    WeakProclet<Shard> shard;
    uint32_t seq;
    std::vector<DataEntry> insert_reqs;
    RemUniquePtr<RobExecutor<ReqBatch, std::optional<ReqBatch>>> flush_executor;
    std::deque<Future<std::optional<typename Shard::ReqBatch>>> flush_futures;

    ShardAndReqs() = default;
    ShardAndReqs(WeakProclet<Shard> s);
    ShardAndReqs(const ShardAndReqs &);
    ShardAndReqs(ShardAndReqs &&) = default;
    ShardAndReqs &operator=(ShardAndReqs &&) = default;

    template <class Archive>
    void save(Archive &ar) const;
    template <class Archive>
    void load(Archive &ar);
  };
  using KeyToShardsMapping = std::multimap<std::optional<Key>, ShardAndReqs>;

  Proclet<ShardMapping> mapping_;
  uint64_t last_mapping_seq_;
  uint64_t mapping_seq_;
  KeyToShardsMapping key_to_shards_;
  std::vector<Val> push_back_reqs_;
  std::unordered_set<ShardAndReqs *> pending_flushes_links_;
  uint64_t num_pending_flushes_;
  uint64_t max_num_vals_;
  uint64_t max_num_data_entries_;
  std::unique_ptr<ReadSkewedLock> rw_lock_;
  template <ShardedDataStructureBased T>
  friend class SealedDS;

  std::size_t __size();
  Val __front()
    requires HasFront<Container>;
  Val __back()
    requires HasBack<Container>;
  template <bool Flush = true, typename D>
  void __insert(D &&entry)
    requires InsertAble<Container>;
  template <typename K>
  bool __erase(K &&k)
    requires EraseAble<Container>;
  template <typename V>
  void __push_front(V &&v)
    requires PushFrontAble<Container>;
  template <bool Flush = true, typename V>
  void __push_back(V &&v)
    requires PushBackAble<Container>;
  template <bool Front, typename RetT, typename F, typename... As>
  RetT run_at_border(F f, As &&...args);
  std::optional<IterVal> __find_data(Key k)
    requires FindDataAble<Container>;
  void reset();
  void set_shard_and_batch_size(uint32_t *max_shard_bytes,
                                uint32_t *max_shard_size,
                                uint32_t *max_batch_size);
  bool flush_one_batch(KeyToShardsMapping::iterator iter, bool drain);
  void handle_rejected_flush_batches(std::vector<ReqBatch> batches);
  std::pair<std::vector<DataEntry>, std::vector<Val>> sync_mapping(
      bool dont_reroute = false);
  void flush_and_sync_mapping();
  std::pair<std::optional<Key>, std::optional<Key>> get_key_range(
      KeyToShardsMapping::iterator iter);
  std::vector<std::tuple<std::optional<Key>, std::size_t, WeakProclet<Shard>>>
  get_all_shards_info();
  void seal();
  void unseal();
  void reroute_reqs(std::vector<DataEntry> insert_reqs,
                    std::vector<Val> push_back_reqs);
  template <typename... S1s>
  void __for_all(auto *fn, S1s &&...states);
  template <class Archive>
  void __save(Archive &ar);
  void update_max_num_data_entries(KeyToShardsMapping::iterator iter);
  void update_max_num_vals();
  bool pop_flush_future(
      std::deque<Future<std::optional<ReqBatch>>> *flush_futures,
      std::vector<ReqBatch> *rejected_batches);
  std::vector<ReqBatch> wait_for_pending_flushes(bool drain);
};

template <PushBackAble Container, BoolIntegral LL>
class BackInsertIterator {
 public:
  using Val = ShardedDataStructure<Container, LL>::Val;

  BackInsertIterator(ShardedDataStructure<Container, LL> &ds);
  BackInsertIterator<Container, LL> &operator++();
  BackInsertIterator<Container, LL> &operator*();
  BackInsertIterator<Container, LL> &operator=(const Val &val);
  BackInsertIterator<Container, LL> &operator=(Val &&val);

 private:
  ShardedDataStructure<Container, LL> &ds_;
};

template <PushBackAble Container, BoolIntegral LL>
BackInsertIterator<Container, LL> back_inserter(
    ShardedDataStructure<Container, LL> &sharded_ds);

}  // namespace nu

#include "nu/impl/sharded_ds.ipp"
