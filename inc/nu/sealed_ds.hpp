#pragma once

#include <boost/circular_buffer.hpp>
#include <cmath>
#include <deque>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>

#include "container.hpp"
#include "nu/proclet.hpp"
#include "nu/rem_unique_ptr.hpp"
#include "nu/sharded_ds.hpp"
#include "nu/type_traits.hpp"
#include "nu/utils/future.hpp"
#include "nu/utils/rob_executor.hpp"

namespace nu {

template <ShardedDataStructureBased T, bool Fwd>
class GeneralSealedDSConstIterator {
 private:
  using IterVal = T::Shard::GeneralContainer::IterVal;
  using Container = T::Shard::GeneralContainer;
  using ContainerIter =
      std::conditional_t<Fwd, typename Container::ConstIterator,
                         typename Container::ConstReverseIterator>;

 public:
  GeneralSealedDSConstIterator();
  GeneralSealedDSConstIterator(const GeneralSealedDSConstIterator &);
  GeneralSealedDSConstIterator &operator=(const GeneralSealedDSConstIterator &);
  GeneralSealedDSConstIterator(GeneralSealedDSConstIterator &&) noexcept;
  GeneralSealedDSConstIterator &operator=(
      GeneralSealedDSConstIterator &&) noexcept;
  bool operator==(const GeneralSealedDSConstIterator &) const;
  GeneralSealedDSConstIterator &
  operator++() requires PreIncrementable<ContainerIter>;
  GeneralSealedDSConstIterator &
  operator--() requires PreDecrementable<ContainerIter>;
  GeneralSealedDSConstIterator operator++(int) = delete;
  GeneralSealedDSConstIterator operator--(int) = delete;
  const IterVal &operator*() const;
  const IterVal *operator->() const;

  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void save_move(Archive &ar);
  template <class Archive>
  void load(Archive &ar);

 private:
  using Shard = T::Shard;
  using ShardsVec = std::vector<WeakProclet<Shard>>;
  using ShardsVecIter =
      std::conditional_t<Fwd, typename ShardsVec::iterator,
                         typename ShardsVec::reverse_iterator>;
  constexpr static bool kContiguous =
      Fwd ? Container::kContiguousIterator
          : Container::kContiguousReverseIterator;

  class Block {
   public:
    using PrefetchedVec =
        std::conditional_t<kContiguous, std::vector<IterVal>,
                           std::vector<std::pair<IterVal, ContainerIter>>>;
    using Prefetched =
        std::conditional_t<kContiguous, std::pair<PrefetchedVec, ContainerIter>,
                           PrefetchedVec>;
    using ConstIterator =
        std::conditional_t<kContiguous, typename PrefetchedVec::const_iterator,
                           typename PrefetchedVec::const_iterator>;
    using ConstReverseIterator =
        std::conditional_t<kContiguous,
                           typename PrefetchedVec::const_reverse_iterator,
                           typename PrefetchedVec::const_reverse_iterator>;

    Block();
    Block(Prefetched &&prefetched);
    Block(IterVal v, ContainerIter container_iter);
    Block(const Block &);
    Block &operator=(const Block &);
    Block(Block &&);
    Block &operator=(Block &&);
    operator bool() const;
    bool empty() const;
    ConstIterator cbegin() const;
    ConstReverseIterator crbegin() const;
    ConstIterator cend() const;
    ContainerIter get_front_container_iter() const;
    ContainerIter get_back_container_iter() const;
    auto to_gid(ConstIterator iter) const;
    auto to_gid(ConstReverseIterator iter) const;

    static Block shard_front_block(ShardsVecIter shards_iter);
    static Block shard_back_block(ShardsVecIter shards_iter);
    static Block shard_end_block(ShardsVecIter shards_iter);

    Prefetched prefetched;
  };

  struct PrefetchReq {
    std::optional<ContainerIter> iter_update;
    bool next;

    template <class Archive>
    void serialize(Archive &ar);
  };

  constexpr static uint32_t kMaxNumInflightPrefetches = 8;
  constexpr static uint32_t kPrefetchEntrySize =
      kContiguous ? sizeof(IterVal) : sizeof(std::pair<IterVal, ContainerIter>);
  constexpr static uint32_t kPrefetchSizePerThread =
      (64 << 10) / kPrefetchEntrySize;

  std::shared_ptr<ShardsVec> shards_;
  ShardsVecIter shards_iter_;
  Block block_;
  Block::ConstIterator block_iter_;
  RemUniquePtr<RobExecutor<PrefetchReq, typename Block::Prefetched>>
      prefetch_executor_;
  boost::circular_buffer<std::variant<Future<Block>, Block>>
      prefetched_next_blocks_;
  boost::circular_buffer<std::variant<Future<Block>, Block>>
      prefetched_prev_blocks_;
  int32_t prefetch_seq_;

  template <ShardedDataStructureBased U>
  friend class SealedDS;

  template <typename It>
  friend class Range;

  GeneralSealedDSConstIterator(std::shared_ptr<ShardsVec> &shards,
                               bool is_begin);
  GeneralSealedDSConstIterator(std::shared_ptr<ShardsVec> &shards,
                               ShardsVecIter shards_iter, IterVal val,
                               ContainerIter container_iter);
  ShardsVecIter shards_vec_begin() const;
  ShardsVecIter shards_vec_end() const;
  void allocate_prefetch_executor();
  Future<Block> submit_prefetch_req(PrefetchReq prefetch_req);
  void inc_slow_path();
  void dec_slow_path();
  static Block::Prefetched prefetch_next_block(Shard *shard,
                                               ContainerIter *container_iter);
  static Block::Prefetched prefetch_prev_block(Shard *shard,
                                               ContainerIter *container_iter);
  static Block unwrap_block_variant(
      std::variant<Future<Block>, Block> *variant);
};

template <ShardedDataStructureBased T>
class SealedDS {
 public:
  using ConstIterator = GeneralSealedDSConstIterator<T, true>;
  using ConstReverseIterator = GeneralSealedDSConstIterator<T, false>;

  SealedDS(SealedDS &&) = default;
  SealedDS &operator=(SealedDS &&) = delete;
  SealedDS(const SealedDS &) = delete;
  SealedDS &operator=(const SealedDS &) = delete;
  ~SealedDS();
  const ConstIterator &cbegin() const requires ConstIterable<typename T::Shard>;
  const ConstIterator &cend() const requires ConstIterable<typename T::Shard>;
  const ConstReverseIterator &rbegin() const
      requires ConstReverseIterable<typename T::Shard>;
  const ConstReverseIterator &rend() const
      requires ConstReverseIterable<typename T::Shard>;
  // Useful for implementing range-based for loop.
  const ConstIterator &begin() const requires ConstIterable<typename T::Shard>;
  const ConstIterator &end() const requires ConstIterable<typename T::Shard>;
  const ConstReverseIterator &crbegin() const
      requires ConstReverseIterable<typename T::Shard>;
  const ConstReverseIterator &crend() const
      requires ConstReverseIterable<typename T::Shard>;

  bool empty() const;
  std::size_t size() const;
  ConstIterator find_iter(T::Key k) const;
  std::optional<typename T::IterVal> find_data_by_order(
      std::size_t order) requires FindableByOrder<typename T::ContainerImpl>;

 private:
  using Shard = T::Shard;
  using ShardsVec = std::vector<WeakProclet<Shard>>;

  T t_;
  std::vector<std::size_t> prefix_sum_sizes_;
  std::vector<std::optional<typename T::Key>> keys_;
  std::shared_ptr<ShardsVec> shards_;
  ConstIterator cbegin_;
  ConstIterator cend_;
  ConstReverseIterator crbegin_;
  ConstReverseIterator crend_;

  SealedDS(T &&t);
  ConstIterator __find_iter(T::Key k);
  T &&unseal();
  ShardsVec::iterator search_shard(T::Key k);
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
