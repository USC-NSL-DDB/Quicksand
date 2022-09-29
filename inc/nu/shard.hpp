#pragma once

#include <cereal/types/tuple.hpp>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "nu/container.hpp"

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

  GeneralShard(WeakProclet<ShardingMapping> mapping, uint32_t max_shard_size);
  GeneralShard(WeakProclet<ShardingMapping> mapping, uint32_t max_shard_size,
               std::optional<Key> l_key, std::optional<Key> r_key,
               std::size_t capacity);
  ~GeneralShard();
  void set_range_and_data(std::optional<Key> l_key, std::optional<Key> r_key,
                          Container container);
  bool try_emplace(std::optional<Key> l_key, std::optional<Key> r_key, Pair p);
  bool try_emplace_back(std::optional<Key> l_key, std::optional<Key> r_key,
                        Val v) requires EmplaceBackAble<Container>;
  bool try_handle_batch(std::optional<Key> l_key, std::optional<Key> r_key,
                        std::vector<Val> emplace_back_reqs,
                        std::vector<std::pair<Key, Val>> emplace_reqs);
  std::pair<bool, std::optional<IterVal>> find_val(
      Key k) requires Findable<Container>;
  std::tuple<bool, Val, ConstIterator> find(Key k) requires Findable<Container>;
  std::vector<std::pair<IterVal, ConstIterator>> get_front_block(
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<std::pair<IterVal, ConstReverseIterator>> get_rfront_block(
     uint32_t block_size) requires ConstReverseIterable<Container>;
  std::vector<std::pair<IterVal, ConstIterator>> get_back_block(
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<std::pair<IterVal, ConstReverseIterator>> get_rback_block(
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::vector<std::pair<IterVal, ConstIterator>> get_block_forward(
      ConstIterator prev_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<std::pair<IterVal, ConstIterator>> get_block_backward(
      ConstIterator succ_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<std::pair<IterVal, ConstReverseIterator>> get_rblock_forward(
      ConstReverseIterator prev_iter,
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::vector<std::pair<IterVal, ConstReverseIterator>> get_rblock_backward(
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
  uint32_t max_shard_size_;
  WeakProclet<ShardingMapping> mapping_;
  std::optional<Key> l_key_;
  std::optional<Key> r_key_;
  Container container_;
  ReaderWriterLock rw_lock_;
  friend class ContainerHandle<Container>;
  friend class ConstContainerHandle<Container>;

  void split();
  bool bad_range(std::optional<Key> l_key, std::optional<Key> r_key);
  void __get_block_forward(
      std::vector<std::pair<IterVal, ConstIterator>> *block,
      ConstIterator prev_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  void __get_rblock_forward(
      std::vector<std::pair<IterVal, ConstReverseIterator>> *block,
      ConstReverseIterator prev_iter,
      uint32_t block_size) requires ConstReverseIterable<Container>;
};

}  // namespace nu

#include "nu/impl/shard.ipp"
