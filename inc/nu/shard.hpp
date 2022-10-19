#pragma once

#include <functional>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "nu/container.hpp"
#include "nu/rem_unique_ptr.hpp"
#include "nu/utils/rob_executor.hpp"

namespace nu {

template <class Shard>
class GeneralShardingMapping;

template <GeneralContainerBased Container>
class GeneralShard;

template <GeneralContainerBased Container>
class ContainerHandle {
 public:
  ContainerHandle(Container *c, GeneralShard<Container> *shard);
  ~ContainerHandle();
  Container *operator->();
  Container &operator*();

 private:
  Container *c_;
  GeneralShard<Container> *shard_;
};

template <GeneralContainerBased Container>
class ConstContainerHandle {
 public:
  ConstContainerHandle(const Container *c, GeneralShard<Container> *shard);
  ~ConstContainerHandle();
  const Container *operator->();
  const Container &operator*();

 private:
  const Container *c_;
  GeneralShard<Container> *shard_;
};

template <class Container>
struct ContainerAndMetadata {
  Container container;
  std::size_t capacity;
  uint64_t container_bucket_size;

  ContainerAndMetadata() = default;
  ContainerAndMetadata(const ContainerAndMetadata &);
  ContainerAndMetadata(ContainerAndMetadata &&) = default;
  ContainerAndMetadata &operator=(ContainerAndMetadata &&) = default;

  template <class Archive>
  void save(Archive &ar) const;

  template <class Archive>
  void load(Archive &ar);
};

template <GeneralContainerBased Container>
class GeneralShard {
 public:
  using Key = Container::Key;
  using Val = Container::Val;
  using IterVal = Container::IterVal;
  using Pair = Container::Pair;
  using ShardingMapping = GeneralShardingMapping<GeneralShard>;
  using GeneralContainer = Container;
  using ConstIterator = Container::ConstIterator;
  using ConstReverseIterator = Container::ConstReverseIterator;

  struct ReqBatch {
    std::optional<Key> l_key;
    std::optional<Key> r_key;
    WeakProclet<GeneralShard> shard;
    std::vector<Val> emplace_back_reqs;
    std::vector<std::pair<Key, Val>> emplace_reqs;

    template <class Archive>
    void serialize(Archive &ar);
  };

  GeneralShard(WeakProclet<ShardingMapping> mapping, uint32_t max_shard_bytes);
  GeneralShard(WeakProclet<ShardingMapping> mapping, uint32_t max_shard_bytes,
               std::optional<Key> l_key, std::optional<Key> r_key,
               bool reserve_space);
  ~GeneralShard();
  void set_range_and_data(
      std::optional<Key> l_key, std::optional<Key> r_key,
      ContainerAndMetadata<Container> container_and_metadata);
  bool try_emplace(std::optional<Key> l_key, std::optional<Key> r_key, Pair p);
  bool try_emplace_back(std::optional<Key> l_key, std::optional<Key> r_key,
                        Val v) requires EmplaceBackAble<Container>;
  std::optional<ReqBatch> try_handle_batch(const ReqBatch &batch);
  std::pair<bool, std::optional<IterVal>> find_val(
      Key k) requires Findable<Container>;
  std::tuple<bool, Val, ConstIterator> find(Key k) requires Findable<Container>;
  std::vector<std::pair<IterVal, ConstIterator>> get_front_block_with_iters(
      uint32_t block_size) requires ConstIterable<Container>;
  std::pair<std::vector<IterVal>, ConstIterator> get_front_block(
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<std::pair<IterVal, ConstReverseIterator>>
  get_rfront_block_with_iters(
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::pair<std::vector<IterVal>, ConstReverseIterator> get_rfront_block(
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::vector<std::pair<IterVal, ConstIterator>> get_back_block_with_iters(
      uint32_t block_size) requires ConstIterable<Container>;
  std::pair<std::vector<IterVal>, ConstIterator> get_back_block(
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<std::pair<IterVal, ConstReverseIterator>>
  get_rback_block_with_iters(
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::pair<std::vector<IterVal>, ConstReverseIterator> get_rback_block(
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::vector<std::pair<IterVal, ConstIterator>> get_next_block_with_iters(
      ConstIterator prev_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<IterVal> get_next_block(
      ConstIterator prev_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<std::pair<IterVal, ConstIterator>> get_prev_block_with_iters(
      ConstIterator succ_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<IterVal> get_prev_block(
      ConstIterator succ_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<std::pair<IterVal, ConstReverseIterator>>
  get_next_rblock_with_iters(
      ConstReverseIterator prev_iter,
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::vector<IterVal> get_next_rblock(
      ConstReverseIterator prev_iter,
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::vector<std::pair<IterVal, ConstReverseIterator>>
  get_prev_rblock_with_iters(
      ConstReverseIterator succ_iter,
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::vector<IterVal> get_prev_rblock(
      ConstReverseIterator succ_iter,
      uint32_t block_size) requires ConstReverseIterable<Container>;
  ConstIterator cbegin() requires ConstIterable<Container>;
  ConstIterator clast() requires ConstIterable<Container>;
  ConstIterator cend() requires ConstIterable<Container>;
  ConstReverseIterator crbegin() requires ConstReverseIterable<Container>;
  ConstReverseIterator crlast() requires ConstReverseIterable<Container>;
  ConstReverseIterator crend() requires ConstReverseIterable<Container>;
  bool empty();
  Container get_container_copy();
  ContainerHandle<Container> get_container_handle();
  ConstContainerHandle<Container> get_const_container_handle();

 private:
  constexpr static uint32_t kReserveProbeSize = 8192;
  constexpr static float kReserveContainerSizeRatio = 0.5;
  constexpr static float kAlmostFullThresh = 0.95;
  constexpr static uint32_t kSlabFragmentationHeadroom = (2 << 20);

  const uint32_t max_shard_bytes_;
  uint32_t real_max_shard_bytes_;
  WeakProclet<ShardingMapping> mapping_;
  std::optional<Key> l_key_;
  std::optional<Key> r_key_;
  Container container_;
  ReaderWriterLock rw_lock_;
  SlabAllocator *slab_;
  uint64_t container_bucket_size_;
  uint64_t initial_slab_usage_;
  std::size_t initial_size_;
  std::size_t size_thresh_;

  friend class ContainerHandle<Container>;
  friend class ConstContainerHandle<Container>;

  void split();
  bool should_split() const;
  bool bad_range(std::optional<Key> l_key, std::optional<Key> r_key);
  uint32_t __get_next_block_with_iters(
      std::vector<std::pair<IterVal, ConstIterator>>::iterator block_iter,
      ConstIterator prev_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  uint32_t __get_next_block(
      std::vector<IterVal>::iterator block_iter, ConstIterator prev_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  uint32_t __get_next_rblock_with_iters(
      std::vector<std::pair<IterVal, ConstReverseIterator>>::iterator block_it,
      ConstReverseIterator prev_iter,
      uint32_t block_size) requires ConstReverseIterable<Container>;
  uint32_t __get_next_rblock(
      std::vector<IterVal>::iterator block_iter, ConstReverseIterator prev_iter,
      uint32_t block_size) requires ConstReverseIterable<Container>;
};

}  // namespace nu

#include "nu/impl/shard.ipp"
