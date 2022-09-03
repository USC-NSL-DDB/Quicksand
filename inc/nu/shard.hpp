#pragma once

#include <optional>
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

  GeneralShard(WeakProclet<ShardingMapping> mapping, uint32_t max_shard_size,
               std::optional<Key> l_key, std::optional<Key> r_key,
               Container container);
  GeneralShard(WeakProclet<ShardingMapping> mapping, uint32_t max_shard_size,
               std::optional<Key> l_key, std::optional<Key> r_key,
               std::size_t capacity);
  ~GeneralShard();
  bool try_emplace(std::optional<Key> l_key, std::optional<Key> r_key, Pair p);
  bool try_emplace_back(std::optional<Key> l_key, std::optional<Key> r_key,
                        Val v) requires EmplaceBackAble<Container>;
  bool try_handle_batch(std::optional<Key> l_key, std::optional<Key> r_key,
                        std::vector<ContainerReq<Key, Val>> reqs);
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
};

}  // namespace nu

#include "nu/impl/shard.ipp"
