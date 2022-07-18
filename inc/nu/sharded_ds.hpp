#pragma once

#include <functional>

#include <list>
#include <map>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "nu/commons.hpp"
#include "nu/proclet.hpp"
#include "nu/type_traits.hpp"
#include "nu/utils/future.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/reader_writer_lock.hpp"
#include "nu/utils/scoped_lock.hpp"

namespace nu {

template <class Impl>
class GeneralContainer {
 public:
  using Key = Impl::Key;
  using Val = Impl::Val;
  using Pair = std::pair<Key, Val>;

  GeneralContainer() : impl_() {}
  GeneralContainer(std::size_t capacity) : impl_(capacity) {}
  GeneralContainer(const GeneralContainer &c) : impl_(c.impl_) {}
  GeneralContainer &operator=(const GeneralContainer &c) {
    impl_ = c.impl_;
    return *this;
  }
  GeneralContainer(GeneralContainer &&c) noexcept : impl_(std::move(c.impl_)) {}
  GeneralContainer &operator=(GeneralContainer &&c) noexcept {
    impl_ = std::move(c.impl_);
    return *this;
  }
  std::size_t size() const { return impl_.size(); }
  std::size_t capacity() const { return impl_.capacity(); }
  bool empty() const { return impl_.empty(); };
  void clear() { impl_.clear(); };
  void emplace(Key k, Val v) { impl_.emplace(std::move(k), std::move(v)); }
  void emplace_batch(GeneralContainer &&c) {
    impl_.emplace_batch(std::move(c.impl_));
  };
  std::pair<Key, GeneralContainer> split() {
    auto [k, impl] = impl_.split();
    GeneralContainer c;
    c.impl_ = std::move(impl);
    return std::make_pair(std::move(k), std::move(c));
  }
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(std::pair<const Key, Val> &, S0s...),
               S1s &&... states) {
    impl_.for_all(fn, std::forward<S1s>(states)...);
  }
  template <class Archive>
  void save(Archive &ar) const {
    impl_.save(ar);
  }
  template <class Archive>
  void load(Archive &ar) {
    impl_.load(ar);
  }
  Impl &unwrap() { return impl_; }

 private:
  Impl impl_;
};

template <class Shard>
class GeneralShardingMapping;

template <class Container>
class GeneralShard {
  static_assert(is_base_of_template_v<Container, GeneralContainer>);

 public:
  using Key = Container::Key;
  using Val = Container::Val;
  using Pair = Container::Pair;
  using ShardingMapping = GeneralShardingMapping<GeneralShard>;

  GeneralShard(WeakProclet<ShardingMapping> mapping, uint32_t max_shard_size,
               std::optional<Key> l_key, std::optional<Key> r_key,
               Container container);
  Container get_container();
  std::pair<ScopedLock<Mutex>, Container *> get_container_ptr();
  bool try_emplace_batch(std::optional<Key> l_key, std::optional<Key> r_key,
                         Container container);

 private:
  uint32_t max_shard_size_;
  WeakProclet<ShardingMapping> mapping_;
  std::optional<Key> l_key_;
  std::optional<Key> r_key_;
  Container container_;
  Mutex mutex_;
};

template <class Shard>
class GeneralShardingMapping {
  static_assert(is_base_of_template_v<Shard, GeneralShard>);

 public:
  using Key = Shard::Key;

  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>>
  get_shards_in_range(std::optional<Key> l_key, std::optional<Key> r_key);
  std::vector<WeakProclet<Shard>> get_all_shards();
  void update_mapping(std::optional<Key> k, Proclet<Shard> shard);

 private:
  std::map<std::optional<Key>, Proclet<Shard>, std::greater<std::optional<Key>>>
      mapping_;
  ReaderWriterLock rw_lock_;
};

template <class Container>
class ShardedDataStructure {
  static_assert(is_base_of_template_v<Container, GeneralContainer>);

 public:
  using Key = Container::Key;
  using Val = Container::Val;
  using Pair = Container::Pair;
  using Shard = GeneralShard<Container>;
  using ShardingMapping = GeneralShardingMapping<Shard>;

  template <typename K1, typename V1>
  void emplace(K1 &&k1, V1 &&v1);
  void emplace(Pair &&p);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(std::pair<const Key, Val> &, S0s...),
               S1s &&... states);
  Container collect();
  void flush();
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 protected:
  ShardedDataStructure();
  ShardedDataStructure(std::optional<Key> initial_l_key,
                       std::optional<Key> initial_r_key,
                       uint32_t max_shard_bytes, uint32_t max_batch_bytes);
  ShardedDataStructure(uint64_t num, Key estimated_min_key,
                       std::function<void(Key &, uint64_t)> key_inc_fn,
                       uint32_t max_shard_bytes, uint32_t max_batch_bytes);
  ShardedDataStructure(const ShardedDataStructure &);
  ShardedDataStructure &operator=(const ShardedDataStructure &);
  ShardedDataStructure(ShardedDataStructure &&) noexcept;
  ShardedDataStructure &operator=(ShardedDataStructure &&) noexcept;
  ~ShardedDataStructure();

 private:
  struct Batch {
    WeakProclet<Shard> shard;
    Container container;
    uint32_t *per_ip_batch_size = nullptr;

    Batch();
    Batch(WeakProclet<Shard>);

    template <class Archive>
    void save(Archive &ar) const;
    template <class Archive>
    void load(Archive &ar);
  };

  struct PushDataReq {
    std::optional<Key> l_key;
    std::optional<Key> r_key;
    WeakProclet<Shard> shard;
    Container container;

    template <class Archive>
    void serialize(Archive &ar);
  };

  using KeyToBatchMapping =
      std::map<std::optional<Key>, Batch, std::greater<std::optional<Key>>>;
  using IPToBatchsMapping = std::unordered_map<
      NodeIP,
      std::pair<uint32_t, std::list<typename KeyToBatchMapping::iterator>>>;

  Proclet<ShardingMapping> mapping_;
  uint32_t max_shard_size_;
  uint32_t max_batch_size_;
  KeyToBatchMapping key_to_batch_;
  IPToBatchsMapping ip_to_batchs_;
  Future<std::vector<PushDataReq>> push_future_;
  std::map<NodeIP, Proclet<ErasedType>> node_proxy_shards_;
  ReaderWriterLock rw_lock_;
  std::vector<PushDataReq> rejected_push_reqs_;
  SpinLock spin_;

  void submit_push_data_req(NodeIP ip, std::vector<PushDataReq> reqs);
  void add_batch(std::optional<Key> k, WeakProclet<Shard> shard);
  void bind_batch(NodeIP, const KeyToBatchMapping::iterator &batch);
  std::vector<PushDataReq> gen_push_data_reqs(NodeIP ip);
  WeakProclet<ErasedType> get_node_proxy_shard(NodeIP ip);
  void handle_rejected_push_reqs(std::vector<PushDataReq> &reqs);
};

}  // namespace nu

#include "nu/impl/sharded_ds.ipp"
