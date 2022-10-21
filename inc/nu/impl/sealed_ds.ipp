#include <algorithm>

namespace nu {

template <typename T, bool Fwd>
template <class Archive>
inline void GeneralSealedDSConstIterator<T, Fwd>::PrefetchReq::serialize(
    Archive &ar) {
  ar(iter_update, next);
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::Block::Block() {
  static ContainerIter iter;

  if constexpr (kContiguous) {
    prefetched.second = iter;
  } else {
    prefetched.reserve(1);
    prefetched.data()->second = iter;
  }
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::Block::Block(Prefetched &&data)
    : prefetched(std::move(data)) {}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::Block::Block(
    Val v, ContainerIter container_iter) {
  if constexpr (kContiguous) {
    prefetched.first.emplace_back(v);
    prefetched.second = container_iter;
  } else {
    prefetched.emplace_back(std::move(v), container_iter);
  }
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::Block::Block(const Block &o) {
  *this = o;
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::Block &
GeneralSealedDSConstIterator<T, Fwd>::Block::operator=(const Block &o) {
  prefetched = o.prefetched;
  return *this;
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::Block::Block(Block &&o) {
  *this = std::move(o);
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::Block &
GeneralSealedDSConstIterator<T, Fwd>::Block::operator=(Block &&o) {
  prefetched = std::move(o.prefetched);
  return *this;
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::Block::operator bool() const {
  if constexpr (kContiguous) {
    return !prefetched.first.empty();
  } else {
    return !prefetched.empty();
  }
}

template <typename T, bool Fwd>
inline bool GeneralSealedDSConstIterator<T, Fwd>::Block::empty() const {
  if constexpr (kContiguous) {
    return prefetched.first.empty();
  } else {
    return prefetched.empty();
  }
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::Block::ConstIterator
GeneralSealedDSConstIterator<T, Fwd>::Block::cbegin() const {
  if constexpr (kContiguous) {
    return prefetched.first.cbegin();
  } else {
    return prefetched.cbegin();
  }
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::Block::ConstReverseIterator
GeneralSealedDSConstIterator<T, Fwd>::Block::crbegin() const {
  if constexpr (kContiguous) {
    return prefetched.first.crbegin();
  } else {
    return prefetched.crbegin();
  }
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::Block::ConstIterator
GeneralSealedDSConstIterator<T, Fwd>::Block::cend() const {
  if constexpr (kContiguous) {
    return prefetched.first.cend();
  } else {
    return prefetched.cend();
  }
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::ContainerIter
GeneralSealedDSConstIterator<T, Fwd>::Block::get_front_container_iter() const {
  if constexpr (kContiguous) {
    return prefetched.second;
  } else {
    return prefetched.front().second;
  }
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::ContainerIter
GeneralSealedDSConstIterator<T, Fwd>::Block::get_back_container_iter() const {
  if constexpr (kContiguous) {
    return prefetched.second + prefetched.first.size() - 1;
  } else {
    return prefetched.back().second;
  }
}

template <typename T, bool Fwd>
inline auto GeneralSealedDSConstIterator<T, Fwd>::Block::to_gid(
    ConstIterator iter) const {
  if constexpr (kContiguous) {
    return (iter - prefetched.first.cbegin()) + prefetched.second;
  } else {
    return iter->second;
  }
}

template <typename T, bool Fwd>
inline auto GeneralSealedDSConstIterator<T, Fwd>::Block::to_gid(
    ConstReverseIterator iter) const {
  if constexpr (kContiguous) {
    return (std::to_address(iter) -
            std::to_address(prefetched.first.cbegin())) +
           prefetched.second;
  } else {
    return iter->second;
  }
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::Block::shard_front_block(
    ShardsVecIter shards_iter) {
  Block b;
  if constexpr (Fwd) {
    if constexpr (kContiguous) {
      b.prefetched =
          shards_iter->run(&Shard::get_front_block, kPrefetchSizePerThread);
    } else {
      b.prefetched = shards_iter->run(&Shard::get_front_block_with_iters,
                                      kPrefetchSizePerThread);
    }
  } else {
    if constexpr (kContiguous) {
      b.prefetched =
          shards_iter->run(&Shard::get_rfront_block, kPrefetchSizePerThread);
    } else {
      b.prefetched = shards_iter->run(&Shard::get_rfront_block_with_iters,
                                      kPrefetchSizePerThread);
    }
  }
  return b;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::Block::shard_back_block(
    ShardsVecIter shards_iter) {
  Block b;
  if constexpr (Fwd) {
    if constexpr (kContiguous) {
      b.prefetched =
          shards_iter->run(&Shard::get_back_block, kPrefetchSizePerThread);
    } else {
      b.prefetched = shards_iter->run(&Shard::get_back_block_with_iters,
                                      kPrefetchSizePerThread);
    }
  } else {
    if constexpr (kContiguous) {
      b.prefetched =
          shards_iter->run(&Shard::get_rback_block, kPrefetchSizePerThread);
    } else {
      b.prefetched = shards_iter->run(&Shard::get_rback_block_with_iters,
                                      kPrefetchSizePerThread);
    }
  }
  return b;
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::Block::shard_end_block(
    ShardsVecIter shards_iter) {
  return Block();
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator()
    : block_iter_(block_.cbegin()),
      prefetched_next_blocks_(kMaxNumInflightPrefetches),
      prefetched_prev_blocks_(kMaxNumInflightPrefetches) {}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator(
    std::shared_ptr<ShardsVec> &shards, bool is_begin)
    : shards_(shards),
      prefetched_next_blocks_(kMaxNumInflightPrefetches),
      prefetched_prev_blocks_(kMaxNumInflightPrefetches) {
  if constexpr (Fwd) {
    shards_iter_ = is_begin ? shards->begin() : shards->end();
  } else {
    shards_iter_ = is_begin ? shards->rbegin() : shards->rend();
  }
  block_ = is_begin ? Block::shard_front_block(shards_iter_)
                    : Block::shard_end_block(shards_iter_);
  block_iter_ = is_begin ? block_.cbegin() : block_.cend();
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator(
    std::shared_ptr<ShardsVec> &shards, ShardsVecIter shards_iter, Val val,
    ContainerIter iter)
    : shards_(shards),
      shards_iter_(shards_iter),
      block_(std::move(val), iter),
      block_iter_(block_.cbegin()),
      prefetched_next_blocks_(kMaxNumInflightPrefetches),
      prefetched_prev_blocks_(kMaxNumInflightPrefetches) {}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator(
    const GeneralSealedDSConstIterator &o)
    : prefetched_next_blocks_(kMaxNumInflightPrefetches),
      prefetched_prev_blocks_(kMaxNumInflightPrefetches) {
  *this = o;
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>
    &GeneralSealedDSConstIterator<T, Fwd>::operator=(
        const GeneralSealedDSConstIterator &o) {
  shards_ = o.shards_;
  shards_iter_ = o.shards_iter_;
  block_ = o.block_;
  block_iter_ = block_.cbegin() + (o.block_iter_ - o.block_.cbegin());
  return *this;
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator(
    GeneralSealedDSConstIterator &&o) noexcept
    : prefetched_next_blocks_(kMaxNumInflightPrefetches),
      prefetched_prev_blocks_(kMaxNumInflightPrefetches) {
  *this = std::move(o);
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>
    &GeneralSealedDSConstIterator<T, Fwd>::operator=(
        GeneralSealedDSConstIterator &&o) noexcept {
  shards_ = std::move(o.shards_);
  shards_iter_ = std::move(o.shards_iter_);
  block_ = std::move(o.block_);
  block_iter_ = std::move(o.block_iter_);
  prefetched_next_blocks_ = std::move(o.prefetched_next_blocks_);
  prefetched_prev_blocks_ = std::move(prefetched_prev_blocks_);
  prefetch_executor_ = std::move(o.prefetch_executor_);
  prefetch_seq_ = o.prefetch_seq_;
  return *this;
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::ShardsVecIter
GeneralSealedDSConstIterator<T, Fwd>::shards_vec_begin() const {
  if constexpr (Fwd) {
    return shards_->begin();
  } else {
    return shards_->rbegin();
  }
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::ShardsVecIter
GeneralSealedDSConstIterator<T, Fwd>::shards_vec_end() const {
  if constexpr (Fwd) {
    return shards_->end();
  } else {
    return shards_->rend();
  }
}

template <typename T, bool Fwd>
[[gnu::always_inline]] inline bool
GeneralSealedDSConstIterator<T, Fwd>::operator==(
    const GeneralSealedDSConstIterator &o) const {
  return block_.to_gid(block_iter_) == o.block_.to_gid(o.block_iter_);
}

template <typename T, bool Fwd>
inline GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::unwrap_block_variant(
    std::variant<Future<Block>, Block> *variant) {
  return std::visit(
      []<typename Arg>(Arg &arg) {
        if constexpr (std::is_same_v<Arg, Block>) {
          return std::move(arg);
        } else {
          return std::move(arg.get());
        }
      },
      *variant);
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::Prefetched
GeneralSealedDSConstIterator<T, Fwd>::prefetch_next_block(
    Shard *shard, ContainerIter *container_iter) {
  typename Block::Prefetched prefetched;

  if constexpr (kContiguous) {
    if constexpr (Fwd) {
      prefetched.first =
          shard->get_next_block(*container_iter, kPrefetchSizePerThread);
    } else {
      prefetched.first =
          shard->get_next_rblock(*container_iter, kPrefetchSizePerThread);
    }
    prefetched.second = *container_iter + 1;
    *container_iter += prefetched.first.size();
  } else {
    if constexpr (Fwd) {
      prefetched = shard->get_next_block_with_iters(*container_iter,
                                                    kPrefetchSizePerThread);
    } else {
      prefetched = shard->get_next_rblock_with_iters(*container_iter,
                                                     kPrefetchSizePerThread);
    }
    if (!prefetched.empty()) {
      *container_iter = prefetched.back().second;
    }
  }
  return prefetched;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::Prefetched
GeneralSealedDSConstIterator<T, Fwd>::prefetch_prev_block(
    Shard *shard, ContainerIter *container_iter) {
  typename Block::Prefetched prefetched;

  if constexpr (kContiguous) {
    if constexpr (Fwd) {
      prefetched.first =
          shard->get_prev_block(*container_iter, kPrefetchSizePerThread);
    } else {
      prefetched.first =
          shard->get_prev_rblock(*container_iter, kPrefetchSizePerThread);
    }
    *container_iter -= prefetched.first.size();
    prefetched.second = *container_iter;
  } else {
    if constexpr (Fwd) {
      prefetched = shard->get_prev_block_with_iters(
          *container_iter, kPrefetchSizePerThread);
    } else {
      prefetched = shard->get_prev_rblock_with_iters(
          *container_iter, kPrefetchSizePerThread);
    }
    if (!prefetched.empty()) {
      *container_iter = prefetched.front().second;
    }
  }
  return prefetched;
}

template <typename T, bool Fwd>
void GeneralSealedDSConstIterator<T, Fwd>::allocate_prefetch_executor() {
  prefetch_executor_ = shards_iter_->run(+[](Shard &shard) {
    auto states = std::make_unique<std::pair<Shard *const, ContainerIter>>(
        &shard, ContainerIter());
    return make_rem_unique<
        RobExecutor<PrefetchReq, typename Block::Prefetched>>(
        [states = std::move(states)](const PrefetchReq &req) {
          if (req.iter_update) {
            states->second = *req.iter_update;
          }
          if (req.next) {
            if constexpr (PreIncrementable<ContainerIter>) {
              return prefetch_next_block(states->first, &states->second);
            }
            BUG();
          } else {
            if constexpr (PreDecrementable<ContainerIter>) {
              return prefetch_prev_block(states->first, &states->second);
            }
	    BUG();
          }
        },
        kMaxNumInflightPrefetches);
  });
}

template <typename T, bool Fwd>
Future<typename GeneralSealedDSConstIterator<T, Fwd>::Block>
GeneralSealedDSConstIterator<T, Fwd>::submit_prefetch_req(
    PrefetchReq prefetch_req) {
  uint32_t seq;

  if (prefetch_req.next) {
    seq = prefetch_seq_++;
  } else {
    seq = -prefetch_seq_--;
  }

  return nu::async([&, seq, prefetch_req = std::move(prefetch_req)] {
    return Block(prefetch_executor_.run(
        +[](RobExecutor<PrefetchReq, typename Block::Prefetched> &rob_executor,
            uint32_t seq, PrefetchReq prefetch_req) {
          return rob_executor.submit(seq, std::move(prefetch_req));
        },
        seq, prefetch_req));
  });
}

template <typename T, bool Fwd>
[[gnu::always_inline]] inline GeneralSealedDSConstIterator<T, Fwd>
    &GeneralSealedDSConstIterator<
        T, Fwd>::operator++() requires PreIncrementable<ContainerIter> {
  if (unlikely(++block_iter_ == block_.cend())) {
    inc_slow_path();
  }
  return *this;
}

template <typename T, bool Fwd>
void GeneralSealedDSConstIterator<T, Fwd>::inc_slow_path() {
  if (unlikely(!prefetch_executor_)) {
    allocate_prefetch_executor();
    prefetch_seq_ = -1;
  }

  if (unlikely(prefetch_seq_ < 0)) {
    prefetch_seq_ = 0;
    prefetched_next_blocks_.push_back(submit_prefetch_req(
        PrefetchReq{block_.get_back_container_iter(), true}));
    while (prefetched_next_blocks_.size() < kMaxNumInflightPrefetches) {
      prefetched_next_blocks_.push_back(
          submit_prefetch_req(PrefetchReq{std::nullopt, true}));
    }
  }

  prefetched_prev_blocks_.push_back(std::move(block_));
  block_ = unwrap_block_variant(&prefetched_next_blocks_.front());
  prefetched_next_blocks_.pop_front();
  prefetched_next_blocks_.push_back(
      submit_prefetch_req(PrefetchReq{std::nullopt, true}));

  if (unlikely(!block_)) {
    prefetched_next_blocks_.clear();
    prefetched_prev_blocks_.clear();
    prefetch_executor_.reset();
    if (likely(++shards_iter_ != shards_vec_end())) {
      block_ = Block::shard_front_block(shards_iter_);
    } else {
      block_ = Block();
    }
  }

  block_iter_ = block_.cbegin();
}

template <typename T, bool Fwd>
[[gnu::always_inline]] inline GeneralSealedDSConstIterator<T, Fwd>
    &GeneralSealedDSConstIterator<
        T, Fwd>::operator--() requires PreDecrementable<ContainerIter> {
  if (unlikely(block_iter_ == block_.cbegin())) {
    dec_slow_path();
  } else {
    block_iter_--;
  }
  return *this;
}

template <typename T, bool Fwd>
void GeneralSealedDSConstIterator<T, Fwd>::dec_slow_path() {
  if (unlikely(shards_iter_ == shards_vec_end())) {
    goto go_prev_shard;
  }

  if (unlikely(!prefetch_executor_)) {
    allocate_prefetch_executor();
    prefetch_seq_ = 1;
  }

  if (unlikely(prefetch_seq_ > 0)) {
    prefetch_seq_ = 0;
    prefetched_prev_blocks_.push_front(submit_prefetch_req(
        PrefetchReq{block_.get_front_container_iter(), false}));
    while (prefetched_prev_blocks_.size() < kMaxNumInflightPrefetches) {
      prefetched_prev_blocks_.push_front(
          submit_prefetch_req(PrefetchReq{std::nullopt, false}));
    }
  }

  prefetched_next_blocks_.push_front(std::move(block_));
  block_ = unwrap_block_variant(&prefetched_prev_blocks_.back());
  prefetched_prev_blocks_.pop_back();
  prefetched_prev_blocks_.push_front(
      submit_prefetch_req(PrefetchReq{std::nullopt, false}));

go_prev_shard:
  if (unlikely(!block_)) {
    prefetched_prev_blocks_.clear();
    prefetched_next_blocks_.clear();
    prefetch_executor_.reset();
    BUG_ON(shards_iter_-- == shards_vec_begin());
    block_ = Block::shard_back_block(shards_iter_);
  }

  block_iter_ = --block_.cend();
}

template <typename T, bool Fwd>
[[gnu::always_inline]] inline const GeneralSealedDSConstIterator<T, Fwd>::Val &
GeneralSealedDSConstIterator<T, Fwd>::operator*() const {
  if constexpr (kContiguous) {
    return *block_iter_;
  } else {
    return block_iter_->first;
  }
}

template <typename T, bool Fwd>
[[gnu::always_inline]] inline const GeneralSealedDSConstIterator<T, Fwd>::Val *
GeneralSealedDSConstIterator<T, Fwd>::operator->() const {
  if constexpr (kContiguous) {
    return std::to_address(block_iter_);
  } else {
    return &block_iter_->first;
  }
}

template <typename T, bool Fwd>
template <class Archive>
inline void GeneralSealedDSConstIterator<T, Fwd>::save(Archive &ar) const {
  uint64_t shards_offset = shards_iter_ - shards_vec_begin();
  uint64_t block_offset = block_iter_ - block_.cbegin();
  ar(shards_, shards_offset, block_.prefetched, block_offset,
     decltype(prefetch_executor_)(), prefetch_seq_);
}

template <typename T, bool Fwd>
template <class Archive>
inline void GeneralSealedDSConstIterator<T, Fwd>::save_move(Archive &ar) {
  uint64_t shards_offset = shards_iter_ - shards_vec_begin();
  uint64_t block_offset = block_iter_ - block_.cbegin();
  ar(shards_, shards_offset, block_.prefetched, block_offset,
     std::move(prefetch_executor_), prefetch_seq_);
}

template <typename T, bool Fwd>
template <class Archive>
inline void GeneralSealedDSConstIterator<T, Fwd>::load(Archive &ar) {
  uint64_t shards_offset, block_offset;
  ar(shards_, shards_offset, block_.prefetched, block_offset,
     prefetch_executor_, prefetch_seq_);
  shards_iter_ = shards_vec_begin() + shards_offset;
  block_iter_ = block_.cbegin() + block_offset;
}

template <typename T>
SealedDS<T>::SealedDS(T &&t) : t_(std::move(t)) {
  t_.seal();

  ShardsVec shards;
  std::tie(keys_, shards) = t_.get_all_non_empty_shards();
  shards_ = std::make_shared<ShardsVec>(shards);

  if constexpr (ConstIterable<typename T::Shard>) {
    cbegin_ = ConstIterator(shards_, true);
    cend_ = ConstIterator(shards_, false);
  }
  if constexpr (ConstReverseIterable<typename T::Shard>) {
    crbegin_ = ConstReverseIterator(shards_, true);
    crend_ = ConstReverseIterator(shards_, false);
  }
}

template <typename T>
inline SealedDS<T>::~SealedDS() {
  if (shards_) {
    t_.unseal();
  }
}

template <typename T>
inline T &&SealedDS<T>::unseal() {
  t_.unseal();
  shards_.reset();
  return std::move(t_);
}

template <typename T>
inline const SealedDS<T>::ConstIterator &SealedDS<T>::begin() const
    requires ConstIterable<typename T::Shard> {
  return cbegin_;
}

template <typename T>
inline const SealedDS<T>::ConstIterator &SealedDS<T>::cbegin() const
    requires ConstIterable<typename T::Shard> {
  return cbegin_;
}

template <typename T>
inline const SealedDS<T>::ConstIterator &SealedDS<T>::end() const
    requires ConstIterable<typename T::Shard> {
  return cend_;
}

template <typename T>
inline const SealedDS<T>::ConstIterator &SealedDS<T>::cend() const
    requires ConstIterable<typename T::Shard> {
  return cend_;
}

template <typename T>
inline const SealedDS<T>::ConstReverseIterator &SealedDS<T>::rbegin() const
    requires ConstReverseIterable<typename T::Shard> {
  return crbegin_;
}

template <typename T>
inline const SealedDS<T>::ConstReverseIterator &SealedDS<T>::crbegin() const
    requires ConstReverseIterable<typename T::Shard> {
  return crbegin_;
}

template <typename T>
inline const SealedDS<T>::ConstReverseIterator &SealedDS<T>::rend() const
    requires ConstReverseIterable<typename T::Shard> {
  return crend_;
}

template <typename T>
inline const SealedDS<T>::ConstReverseIterator &SealedDS<T>::crend() const
    requires ConstReverseIterable<typename T::Shard> {
  return crend_;
}

template <typename T>
inline bool SealedDS<T>::empty() const {
  return !size();
}

template <typename T>
inline std::size_t SealedDS<T>::size() const {
  return const_cast<SealedDS *>(this)->__size();
}

template <typename T>
inline SealedDS<T>::ConstIterator SealedDS<T>::find_iter(T::Key k) const {
  return const_cast<SealedDS *>(this)->__find_iter(std::move(k));
}

template <typename T>
inline SealedDS<T>::ShardsVec::iterator SealedDS<T>::search_shard(T::Key k) {
  auto idx =
      std::upper_bound(keys_.begin(), keys_.end(), k) - keys_.begin() - 1;
  return shards_->begin() + idx;
}

template <typename T>
inline SealedDS<T>::ConstIterator SealedDS<T>::__find_iter(T::Key k) {
  auto shard_iter = search_shard(k);
  auto tuple = shard_iter->run(&Shard::find, k);
  BUG_ON(!std::get<0>(tuple));
  return ConstIterator(shards_, shard_iter, std::move(std::get<1>(tuple)),
                       std::move(std::get<2>(tuple)));
}

template <typename T>
inline std::size_t SealedDS<T>::__size() {
  if (unlikely(!size_)) {
    size_ = t_.size();
  }
  return *size_;
}

template <typename T>
inline SealedDS<T> to_sealed_ds(T &&t) {
  return SealedDS<T>(std::move(t));
}

template <typename T>
inline T to_unsealed_ds(SealedDS<T> &&sealed) {
  return sealed.unseal();
}

}  // namespace nu
