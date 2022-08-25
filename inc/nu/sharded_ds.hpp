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
  using ConstIterator = Impl::ConstIterator;
  using ConstReverseIterator = Impl::ConstReverseIterator;

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
  std::optional<Val> find_val(Key k) { return impl_.find_val(std::move(k)); }
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
  Impl &unwrap() { return impl_; }
  ConstIterator cbegin() const { return impl_.cbegin(); }
  ConstIterator cend() const { return impl_.cend(); }
  ConstReverseIterator crbegin() const { return impl_.crbegin(); }
  ConstReverseIterator crend() const { return impl_.crend(); }
  template <class Archive>
  void save(Archive &ar) const {
    impl_.save(ar);
  }
  template <class Archive>
  void load(Archive &ar) {
    impl_.load(ar);
  }

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
  using GeneralContainer = Container;
  using ConstIterator = Container::ConstIterator;
  using ConstReverseIterator = Container::ConstReverseIterator;

  GeneralShard(WeakProclet<ShardingMapping> mapping, uint32_t max_shard_size,
               std::optional<Key> l_key, std::optional<Key> r_key,
               Container container);
  GeneralShard(WeakProclet<ShardingMapping> mapping, uint32_t max_shard_size,
               std::optional<Key> l_key, std::optional<Key> r_key,
               std::size_t capacity);
  Container get_container();
  std::pair<ScopedLock<Mutex>, Container *> get_container_ptr();
  bool try_emplace(std::optional<Key> l_key, std::optional<Key> r_key, Pair p);
  bool try_emplace_batch(std::optional<Key> l_key, std::optional<Key> r_key,
                         Container container);
  std::pair<bool, std::optional<Val>> find_val(Key k);
  std::optional<ConstIterator> inc_iter(ConstIterator iter);
  std::optional<ConstIterator> dec_iter(ConstIterator iter);
  ConstIterator cbegin();
  ConstIterator clast();
  ConstIterator cend();
  bool empty();

 private:
  uint32_t max_shard_size_;
  WeakProclet<ShardingMapping> mapping_;
  std::optional<Key> l_key_;
  std::optional<Key> r_key_;
  Container container_;
  Mutex mutex_;

  void split();
  bool bad_range(std::optional<Key> l_key, std::optional<Key> r_key);
};

template <class Shard>
class GeneralShardingMapping {
  static_assert(is_base_of_template_v<Shard, GeneralShard>);

 public:
  using Key = Shard::Key;

  GeneralShardingMapping();
  ~GeneralShardingMapping();
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>>
  get_shards_in_range(std::optional<Key> l_key, std::optional<Key> r_key);
  std::optional<WeakProclet<Shard>> get_shard_for_key(std::optional<Key> key);
  void update_mapping(std::optional<Key> k, Proclet<Shard> shard);
  void inc_ref_cnt();
  void dec_ref_cnt();
  void seal();
  void unseal();

 private:
  std::map<std::optional<Key>, Proclet<Shard>> mapping_;
  ReaderWriterLock rw_lock_;
  uint32_t ref_cnt_;
  Mutex ref_cnt_mu_;
  CondVar ref_cnt_cv_;
};

template <class Container, class LL>
class ShardedDataStructure {
  static_assert(is_base_of_template_v<Container, GeneralContainer>);
  static_assert(std::is_same_v<LL, std::bool_constant<false>> ||
                std::is_same_v<LL, std::bool_constant<true>>);

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

  template <typename K1, typename V1>
  void emplace(K1 &&k1, V1 &&v1);
  void emplace(Pair &&p);
  std::optional<Val> find_val(Key k);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  Container collect();
  std::size_t size();
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

  using KeyToShardsMapping =
      std::map<std::optional<Key>, std::pair<WeakProclet<Shard>, Container>>;

  struct FlushBatchReq {
    std::optional<Key> l_key;
    std::optional<Key> r_key;
    WeakProclet<Shard> shard;
    Container batch;

    template <class Archive>
    void serialize(Archive &ar);
  };

  Proclet<ShardingMapping> mapping_;
  uint32_t max_shard_size_;
  uint32_t max_batch_size_;
  KeyToShardsMapping key_to_shards_;
  Future<std::optional<FlushBatchReq>> flush_future_;
  ReaderWriterLock rw_lock_;
  template <typename T>
  friend class SealedDS;

  void set_shard_and_batch_size();
  bool flush_one_batch(KeyToShardsMapping::iterator iter);
  void handle_rejected_flush_req(FlushBatchReq &req);
  void sync_mapping(std::optional<Key> l_key, std::optional<Key> r_key);
  std::pair<std::optional<Key>, std::optional<Key>> get_key_range(
      KeyToShardsMapping::iterator iter);
  std::vector<WeakProclet<Shard>> get_all_non_empty_shards();
  void seal();
  void unseal();
};

}  // namespace nu

#include "nu/impl/sharded_ds.ipp"
