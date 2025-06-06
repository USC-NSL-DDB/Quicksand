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
#include "nu/cont_ds_range.hpp"
#include "nu/proclet.hpp"
#include "nu/rem_unique_ptr.hpp"
#include "nu/sharded_ds.hpp"
#include "nu/sharded_ds_range.hpp"
#include "nu/type_traits.hpp"
#include "nu/utils/future.hpp"
#include "nu/utils/rob_executor.hpp"

namespace nu {

template <GeneralShardBased Shard, bool Fwd>
class GeneralSealedDSConstIterator {
 private:
  using IterVal = Shard::GeneralContainer::IterVal;
  using Container = Shard::GeneralContainer;
  using ContainerIter =
      std::conditional_t<Fwd, typename Container::ConstIterator,
                         typename Container::ConstReverseIterator>;

 public:
  constexpr static bool kContiguous =
      Fwd ? Container::kContiguousIterator
          : Container::kContiguousReverseIterator;

  GeneralSealedDSConstIterator();
  GeneralSealedDSConstIterator(const GeneralSealedDSConstIterator &);
  GeneralSealedDSConstIterator &operator=(const GeneralSealedDSConstIterator &);
  GeneralSealedDSConstIterator(GeneralSealedDSConstIterator &&) noexcept;
  GeneralSealedDSConstIterator &operator=(
      GeneralSealedDSConstIterator &&) noexcept;
  GeneralSealedDSConstIterator deep_copy();
  bool operator==(const GeneralSealedDSConstIterator &) const;
  GeneralSealedDSConstIterator &operator++()
    requires PreIncrementable<ContainerIter>;
  GeneralSealedDSConstIterator &operator--()
    requires PreDecrementable<ContainerIter>;
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
  using ShardsVec = std::vector<WeakProclet<Shard>>;
  using ShardsVecIter =
      std::conditional_t<Fwd, typename ShardsVec::iterator,
                         typename ShardsVec::reverse_iterator>;

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
    uint64_t size() const;
    uint64_t size_bytes() const;
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
    uint32_t cnt;

    template <class Archive>
    void serialize(Archive &ar);
  };

  constexpr static uint32_t kMaxNumInflightPrefetches = 1;
  constexpr static uint32_t kPrefetchBytesPerThread = (64 << 10);

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
  uint32_t prefetch_cnt_per_thread_;

  template <ShardedDataStructureBased U>
  friend class SealedDS;
  template <GeneralShardBased S>
  friend class ShardRange;
  template <GeneralShardBased S>
  friend class ContiguousDSRangeImpl;

  GeneralSealedDSConstIterator(std::shared_ptr<ShardsVec> shards,
                               bool is_begin);
  GeneralSealedDSConstIterator(std::shared_ptr<ShardsVec> shards,
                               ShardsVecIter shards_iter, IterVal val,
                               ContainerIter container_iter);
  auto to_gid() const;
  ShardsVecIter shards_vec_begin() const;
  ShardsVecIter shards_vec_end() const;
  void allocate_prefetch_executor();
  Future<Block> submit_prefetch_req(PrefetchReq prefetch_req);
  void inc_slow_path();
  void dec_slow_path();
  void update_prefetch_cnt(const Block &block);
  static Block::Prefetched prefetch_next_block(Shard *shard,
                                               ContainerIter *container_iter,
                                               uint32_t cnt);
  static Block::Prefetched prefetch_prev_block(Shard *shard,
                                               ContainerIter *container_iter,
                                               uint32_t cnt);
  static Block unwrap_block_variant(
      std::variant<Future<Block>, Block> *variant);
  IterVal &&move_deref();
};

template <ShardedDataStructureBased T>
class SealedDS {
 public:
  using ConstIterator = GeneralSealedDSConstIterator<typename T::Shard, true>;
  using ConstReverseIterator =
      GeneralSealedDSConstIterator<typename T::Shard, false>;

  SealedDS(SealedDS &&) = default;
  SealedDS &operator=(SealedDS &&) = delete;
  SealedDS(const SealedDS &) = delete;
  SealedDS &operator=(const SealedDS &) = delete;
  ~SealedDS();
  const ConstIterator &cbegin() const
    requires ConstIterable<typename T::Shard>;
  const ConstIterator &cend() const
    requires ConstIterable<typename T::Shard>;
  const ConstReverseIterator &rbegin() const
    requires ConstReverseIterable<typename T::Shard>;
  const ConstReverseIterator &rend() const
    requires ConstReverseIterable<typename T::Shard>;
  // Useful for implementing range-based for loop.
  const ConstIterator &begin() const
    requires ConstIterable<typename T::Shard>;
  const ConstIterator &end() const
    requires ConstIterable<typename T::Shard>;
  const ConstReverseIterator &crbegin() const
    requires ConstReverseIterable<typename T::Shard>;
  const ConstReverseIterator &crend() const
    requires ConstReverseIterable<typename T::Shard>;

  bool empty() const;
  std::size_t size() const;
  ConstIterator find_iter(T::Key k) const
    requires FindAble<typename T::ContainerImpl>;
  std::optional<typename T::IterVal> find_data_by_order(std::size_t order)
    requires FindAbleByOrder<typename T::ContainerImpl>;

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
  ConstIterator __find_iter(T::Key k)
    requires FindAble<typename T::ContainerImpl>;
  T &&unseal();
  ShardsVec::iterator search_shard(T::Key k);
  template <typename U>
  friend SealedDS<U> to_sealed_ds(U &&u);
  template <typename U>
  friend U to_unsealed_ds(SealedDS<U> &&sealed);
  template <ShardedDataStructureBased U>
  friend ShardedDSRange<typename U::Shard> make_sharded_ds_range(
      const SealedDS<U> &sealed_ds);
  template <ShardedDataStructureBased U>
  ContiguousDSRange<typename U::Shard> friend make_contiguous_ds_range(
      const SealedDS<U> &sealed_ds)
    requires(SealedDS<U>::ConstIterator::kContiguous);
};

template <typename T>
SealedDS<T> to_sealed_ds(T &&t);
template <typename T>
T to_unsealed_ds(SealedDS<T> &&sealed);

}  // namespace nu

#include "nu/impl/sealed_ds.ipp"
