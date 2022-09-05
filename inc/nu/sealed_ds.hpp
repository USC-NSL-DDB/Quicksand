#pragma once

#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include "nu/proclet.hpp"
#include "nu/sharded_ds.hpp"
#include "nu/type_traits.hpp"
#include "nu/utils/future.hpp"

namespace nu {

template <ShardedDataStructureBased T, bool Fwd>
class GeneralSealedDSConstIterator {
 public:
  using Val = T::Shard::GeneralContainer::IterVal;

  GeneralSealedDSConstIterator();
  GeneralSealedDSConstIterator(const GeneralSealedDSConstIterator &);
  GeneralSealedDSConstIterator &operator=(const GeneralSealedDSConstIterator &);
  GeneralSealedDSConstIterator(GeneralSealedDSConstIterator &&) noexcept;
  GeneralSealedDSConstIterator &operator=(
      GeneralSealedDSConstIterator &&) noexcept;
  bool operator==(const GeneralSealedDSConstIterator &) const;
  GeneralSealedDSConstIterator &operator++();
  GeneralSealedDSConstIterator &operator--();
  GeneralSealedDSConstIterator operator++(int) = delete;
  GeneralSealedDSConstIterator operator--(int) = delete;
  const Val &operator*() const;
  const Val *operator->() const;

  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  using Shard = T::Shard;
  using ShardsVec = std::vector<WeakProclet<Shard>>;
  using ShardsVecIter =
      std::conditional_t<Fwd, typename ShardsVec::iterator,
                         typename ShardsVec::reverse_iterator>;
  using ContainerIter = std::conditional_t<
      Fwd, typename T::Shard::GeneralContainer::ConstIterator,
      typename T::Shard::GeneralContainer::ConstReverseIterator>;
  class Block {
   public:
    using PrefetchedVec = std::vector<std::pair<Val, ContainerIter>>;
    using ConstIterator = PrefetchedVec::const_iterator;
    using ConstReverseIterator = PrefetchedVec::const_reverse_iterator;
    constexpr static uint32_t kSize =
        (256 << 10) / sizeof(std::pair<Val, ContainerIter>);

    Block();
    Block(const Block &);
    Block &operator=(const Block &);
    Block(Block &&);
    Block &operator=(Block &&);
    bool empty() const;
    ConstIterator cbegin() const;
    ConstReverseIterator crbegin() const;
    ConstIterator cend() const;
    Block next_block() const;
    Block prev_block() const;
    ShardsVecIter get_shards_iter() const;

    static Block shard_front_block(ShardsVecIter shards_vec_iter);
    static Block shard_back_block(ShardsVecIter shards_vec_iter);
    static Block shard_end_block(ShardsVecIter shards_vec_iter);

  public:
    ShardsVecIter shards_iter;
    PrefetchedVec prefetched;
  };

  std::shared_ptr<ShardsVec> shards_;
  Block block_;
  Future<Block> prefetched_next_block_;
  Future<Block> prefetched_prev_block_;
  Block::ConstIterator block_iter_;

  template <ShardedDataStructureBased U>
  friend class SealedDS;

  GeneralSealedDSConstIterator(std::shared_ptr<ShardsVec> &shards,
                               bool is_begin);
  ShardsVecIter shards_vec_begin() const;
  ShardsVecIter shards_vec_end() const;
  Block get_next_block(const Block &block);
  Block get_prev_block(const Block &block);
};

template <ShardedDataStructureBased T>
class SealedDS {
 public:
  using ConstIterator = GeneralSealedDSConstIterator<T, true>;
  using ConstReverseIterator = GeneralSealedDSConstIterator<T, false>;

  SealedDS(SealedDS &&);
  SealedDS &operator=(SealedDS &&) = delete;
  SealedDS(const SealedDS &) = delete;
  SealedDS &operator=(const SealedDS &) = delete;
  ~SealedDS();
  ConstIterator cbegin() const requires ConstIterable<typename T::Shard>;
  ConstIterator cend() const requires ConstIterable<typename T::Shard>;
  ConstReverseIterator rbegin() const
      requires ConstReverseIterable<typename T::Shard>;
  ConstReverseIterator rend() const
      requires ConstReverseIterable<typename T::Shard>;
  // Useful for implementing range-based for loop.
  ConstIterator begin() const requires ConstIterable<typename T::Shard>;
  ConstIterator end() const requires ConstIterable<typename T::Shard>;
  ConstReverseIterator crbegin() const
      requires ConstReverseIterable<typename T::Shard>;
  ConstReverseIterator crend() const
      requires ConstReverseIterable<typename T::Shard>;
  std::size_t size() const;

 private:
  using Shard = T::Shard;
  using ShardsVec = std::vector<WeakProclet<Shard>>;

  T t_;
  std::optional<std::size_t> size_;
  std::shared_ptr<ShardsVec> shards_;
  ConstIterator cbegin_;
  ConstIterator cend_;
  ConstReverseIterator crbegin_;
  ConstReverseIterator crend_;

  SealedDS(T &&t);
  std::size_t __size();
  T &&unseal();
  template <typename U>
  friend SealedDS<U> to_sealed_ds(U &&u);
  template <typename U>
  friend U to_unsealed_ds(SealedDS<U> &&sealed);
};

template <typename T>
SealedDS<T> to_sealed_ds(T &&t);
template <typename T>
T to_unsealed_ds(SealedDS<T> &&sealed);

}  // namespace nu

#include "nu/impl/sealed_ds.ipp"
