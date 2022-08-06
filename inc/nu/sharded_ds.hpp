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
template <class Container>
class GeneralShard;

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
  std::optional<Val> find(Key k) { return impl_.find(std::move(k)); }
  std::pair<Key, GeneralContainer> split() {
    auto [k, impl] = impl_.split();
    GeneralContainer c;
    c.impl_ = std::move(impl);
    return std::make_pair(std::move(k), std::move(c));
  }
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states) {
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
  GeneralShard(WeakProclet<ShardingMapping> mapping, uint32_t max_shard_size,
               std::optional<Key> l_key, std::optional<Key> r_key,
               std::size_t capacity);
  Container get_container();
  std::pair<ScopedLock<Mutex>, Container *> get_container_ptr();
  bool try_emplace_batch(std::optional<Key> l_key, std::optional<Key> r_key,
                         Container container);
  std::optional<Val> find(Key k) { return container_.find(std::move(k)); }
  std::optional<Key> l_key() const { return l_key_; }
  std::optional<Key> r_key() const { return r_key_; }

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
  std::optional<WeakProclet<Shard>> get_shard_for_key(std::optional<Key> key);
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
  std::optional<Val> find(Key k);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  Container collect();
  void flush();
  template <class Archive>
  void serialize(Archive &ar);

 protected:
  ShardedDataStructure();
  ShardedDataStructure(uint32_t max_shard_bytes, uint32_t max_batch_bytes);
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

    Batch();
    Batch(WeakProclet<Shard>);

    template <class Archive>
    void save(Archive &ar) const;
    template <class Archive>
    void load(Archive &ar);
  };

  using KeyToBatchMapping =
      std::map<std::optional<Key>, Batch, std::greater<std::optional<Key>>>;

  struct FlushBatchReq {
    std::optional<Key> l_key;
    std::optional<Key> r_key;
    WeakProclet<Shard> shard;
    Container container;

    FlushBatchReq();
    FlushBatchReq(KeyToBatchMapping::iterator iter);
    template <class Archive>
    void serialize(Archive &ar);
  };

  Proclet<ShardingMapping> mapping_;
  uint32_t max_shard_size_;
  uint32_t max_batch_size_;
  KeyToBatchMapping key_to_batch_;
  Future<std::optional<FlushBatchReq>> flush_future_;
  ReaderWriterLock rw_lock_;

  bool flush_one_batch(FlushBatchReq req);
  void handle_rejected_flush_req(FlushBatchReq &req);
};

}  // namespace nu

#include "nu/impl/sharded_ds.ipp"
