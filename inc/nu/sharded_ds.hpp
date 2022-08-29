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
class GeneralContainer;

template <class Impl>
class GeneralLockedContainer;

template <class Impl, class Synchronized>
class GeneralContainerBase {
  static_assert(std::is_same_v<Synchronized, std::bool_constant<false>> ||
                std::is_same_v<Synchronized, std::bool_constant<true>>);

 public:
  using Key = Impl::Key;
  using Val = Impl::Val;
  using IterVal = Impl::IterVal;
  using Pair = std::pair<Key, Val>;
  using ConstIterator = Impl::ConstIterator;
  using ConstReverseIterator = Impl::ConstReverseIterator;
  using ContainerType =
      std::conditional_t<Synchronized::value, GeneralLockedContainer<Impl>,
                         GeneralContainer<Impl>>;

  GeneralContainerBase() : impl_() {}
  GeneralContainerBase(std::size_t capacity) : impl_(capacity) {}
  GeneralContainerBase(const GeneralContainerBase &c) : impl_(c.impl_) {}
  GeneralContainerBase &operator=(const GeneralContainerBase &c) {
    impl_ = c.impl_;
    return *this;
  }
  GeneralContainerBase(GeneralContainerBase &&c) noexcept
      : impl_(std::move(c.impl_)) {}
  GeneralContainerBase &operator=(GeneralContainerBase &&c) noexcept {
    impl_ = std::move(c.impl_);
    return *this;
  }
  std::size_t size() {
    return synchronized<std::size_t>([&]() { return impl_.size(); });
  }
  std::size_t capacity() {
    return synchronized<std::size_t>([&]() { return impl_.capacity(); });
  }
  bool empty() {
    return synchronized<bool>([&]() { return impl_.empty(); });
  };
  void clear() {
    return synchronized<void>([&]() { return impl_.clear(); });
  };
  void emplace(Key k, Val v) {
    synchronized<void>([&]() { impl_.emplace(std::move(k), std::move(v)); });
  }
  void emplace_batch(ContainerType &&c) {
    synchronized<void>([&]() { impl_.emplace_batch(std::move(c.impl_)); });
  };
  std::optional<Val> find_val(Key k) {
    return synchronized<std::optional<Val>>(
        [&]() { return impl_.find_val(std::move(k)); });
  }
  std::pair<Key, ContainerType> split() {
    return synchronized<std::pair<Key, ContainerType>>([&]() {
      auto [k, impl] = impl_.split();
      ContainerType c;
      c.impl_ = std::move(impl);
      return std::make_pair(std::move(k), std::move(c));
    });
  }
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states) {
    synchronized<void>(
        [&]() { impl_.for_all(fn, std::forward<S1s>(states)...); });
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
  friend GeneralShard<GeneralContainer<Impl>>;
  friend GeneralShard<GeneralLockedContainer<Impl>>;

  Impl impl_;
  Mutex mutex_;

  template <typename RetT, typename F>
  inline RetT synchronized(F &&f) {
    if constexpr (Synchronized::value) {
      ScopedLock<Mutex> guard(&mutex_);
      return f();
    } else {
      return f();
    }
  }
};

template <class Impl>
class GeneralContainer : public GeneralContainerBase<Impl, std::false_type> {
 public:
  using Base = GeneralContainerBase<Impl, std::false_type>;

  GeneralContainer() : Base() {}
  GeneralContainer(std::size_t capacity) : Base(capacity) {}
  GeneralContainer(const GeneralContainer &c) : Base(c) {}
  GeneralContainer &operator=(const GeneralContainer &c) noexcept {
    return Base::operator=(static_cast<const Base &>(c));
  }
  GeneralContainer(GeneralContainer &&c) noexcept : Base(std::move(c)) {}
  GeneralContainer &operator=(GeneralContainer &&c) noexcept {
    return static_cast<GeneralContainer &>(Base::operator=(std::move(c)));
  }
};

template <class Impl>
class GeneralLockedContainer
    : public GeneralContainerBase<Impl, std::true_type> {
 public:
  using Base = GeneralContainerBase<Impl, std::true_type>;

  GeneralLockedContainer() : Base() {}
  GeneralLockedContainer(std::size_t capacity) : Base(capacity) {}
  GeneralLockedContainer(const GeneralLockedContainer &c) : Base(c) {}
  GeneralLockedContainer &operator=(const GeneralLockedContainer &c) noexcept {
    return Base::operator=(static_cast<const Base &>(c));
  }
  GeneralLockedContainer(GeneralLockedContainer &&c) noexcept
      : Base(std::move(c)) {}
  GeneralLockedContainer &operator=(GeneralLockedContainer &&c) noexcept {
    return static_cast<GeneralLockedContainer &>(Base::operator=(std::move(c)));
  }
};

template <class Shard>
class GeneralShardingMapping;

template <class Container>
class GeneralShard {
  static_assert(is_base_of_template_v<Container, GeneralContainerBase>);

 public:
  using Key = Container::Key;
  using Val = Container::Val;
  using IterVal = Container::IterVal;
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
  ~GeneralShard();
  bool try_emplace(std::optional<Key> l_key, std::optional<Key> r_key, Pair p);
  bool try_emplace_batch(std::optional<Key> l_key, std::optional<Key> r_key,
                         Container container);
  std::pair<bool, std::optional<Val>> find_val(Key k);
  std::pair<std::vector<IterVal>, ConstIterator> get_block_forward(
      ConstIterator start_iter, uint32_t block_size);
  std::pair<std::vector<IterVal>, ConstIterator> get_block_backward(
      ConstIterator end_iter, uint32_t block_size);
  std::pair<std::vector<IterVal>, ConstReverseIterator> get_rblock_forward(
      ConstReverseIterator start_iter, uint32_t block_size);
  std::pair<std::vector<IterVal>, ConstReverseIterator> get_rblock_backward(
      ConstReverseIterator start_iter, uint32_t block_size);
  ConstIterator cbegin();
  ConstIterator clast();
  ConstIterator cend();
  ConstReverseIterator crbegin();
  ConstReverseIterator crlast();
  ConstReverseIterator crend();
  bool empty();

  class ContainerHandle {
   public:
    ContainerHandle(Container *c, GeneralShard *shard) : c_(c), shard_(shard) {
      shard_->rw_lock_.writer_lock();
    }
    ~ContainerHandle() { shard_->rw_lock_.writer_unlock(); }

    Container *operator->() { return c_; }
    Container &operator*() { return *c_; }

   private:
    Container *c_;
    GeneralShard *shard_;
  };

  class ConstContainerHandle {
   public:
    ConstContainerHandle(const Container *c, GeneralShard *shard)
        : c_(c), shard_(shard) {
      shard_->rw_lock_.reader_lock();
    }
    ~ConstContainerHandle() { shard_->rw_lock_.reader_unlock(); }

    const Container *operator->() { return c_; }
    const Container &operator*() { return *c_; }

   private:
    const Container *c_;
    GeneralShard *shard_;
  };

  Container get_container();
  ContainerHandle get_container_ptr();
  ConstContainerHandle get_const_container_ptr();

 private:
  uint32_t max_shard_size_;
  WeakProclet<ShardingMapping> mapping_;
  std::optional<Key> l_key_;
  std::optional<Key> r_key_;
  Container container_;
  ReaderWriterLock rw_lock_;

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
  static_assert(is_base_of_template_v<Container, GeneralContainerBase>);
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
