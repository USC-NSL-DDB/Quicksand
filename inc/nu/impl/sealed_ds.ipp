#include <algorithm>

#include "nu/cereal.hpp"

namespace nu {

template <GeneralShardBased Shard, bool Fwd>
template <class Archive>
inline void GeneralSealedDSConstIterator<Shard, Fwd>::PrefetchReq::serialize(
    Archive &ar) {
  ar(iter_update, next, cnt);
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::Block::Block() {
  static ContainerIter iter;

  if constexpr (kContiguous) {
    prefetched.second = iter;
  } else {
    prefetched.reserve(1);
    prefetched.data()->second = iter;
  }
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::Block::Block(Prefetched &&data)
    : prefetched(std::move(data)) {}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::Block::Block(
    IterVal v, ContainerIter container_iter) {
  if constexpr (kContiguous) {
    prefetched.first.emplace_back(v);
    prefetched.second = container_iter;
  } else {
    prefetched.emplace_back(std::move(v), container_iter);
  }
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::Block::Block(const Block &o) {
  *this = o;
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::Block &
GeneralSealedDSConstIterator<Shard, Fwd>::Block::operator=(const Block &o) {
  prefetched = o.prefetched;
  return *this;
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::Block::Block(Block &&o) {
  *this = std::move(o);
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::Block &
GeneralSealedDSConstIterator<Shard, Fwd>::Block::operator=(Block &&o) {
  prefetched = std::move(o.prefetched);
  return *this;
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::Block::operator bool() const {
  if constexpr (kContiguous) {
    return !prefetched.first.empty();
  } else {
    return !prefetched.empty();
  }
}

template <GeneralShardBased Shard, bool Fwd>
inline bool GeneralSealedDSConstIterator<Shard, Fwd>::Block::empty() const {
  if constexpr (kContiguous) {
    return prefetched.first.empty();
  } else {
    return prefetched.empty();
  }
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::Block::ConstIterator
GeneralSealedDSConstIterator<Shard, Fwd>::Block::cbegin() const {
  if constexpr (kContiguous) {
    return prefetched.first.cbegin();
  } else {
    return prefetched.cbegin();
  }
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::Block::ConstReverseIterator
GeneralSealedDSConstIterator<Shard, Fwd>::Block::crbegin() const {
  if constexpr (kContiguous) {
    return prefetched.first.crbegin();
  } else {
    return prefetched.crbegin();
  }
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::Block::ConstIterator
GeneralSealedDSConstIterator<Shard, Fwd>::Block::cend() const {
  if constexpr (kContiguous) {
    return prefetched.first.cend();
  } else {
    return prefetched.cend();
  }
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::ContainerIter
GeneralSealedDSConstIterator<Shard, Fwd>::Block::get_front_container_iter()
    const {
  if constexpr (kContiguous) {
    return prefetched.second;
  } else {
    return prefetched.front().second;
  }
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::ContainerIter
GeneralSealedDSConstIterator<Shard, Fwd>::Block::get_back_container_iter()
    const {
  if constexpr (kContiguous) {
    return prefetched.second + prefetched.first.size() - 1;
  } else {
    return prefetched.back().second;
  }
}

template <GeneralShardBased Shard, bool Fwd>
inline uint64_t GeneralSealedDSConstIterator<Shard, Fwd>::Block::size() const {
  if constexpr (kContiguous) {
    return prefetched.first.size();
  } else {
    return prefetched.size();
  }
}

template <GeneralShardBased Shard, bool Fwd>
inline uint64_t GeneralSealedDSConstIterator<Shard, Fwd>::Block::size_bytes()
    const {
  return cereal::get_size(prefetched);
}

template <GeneralShardBased Shard, bool Fwd>
inline auto GeneralSealedDSConstIterator<Shard, Fwd>::Block::to_gid(
    ConstIterator iter) const {
  if constexpr (kContiguous) {
    return (iter - prefetched.first.cbegin()) + prefetched.second;
  } else {
    return iter->second;
  }
}

template <GeneralShardBased Shard, bool Fwd>
inline auto GeneralSealedDSConstIterator<Shard, Fwd>::Block::to_gid(
    ConstReverseIterator iter) const {
  if constexpr (kContiguous) {
    return (std::to_address(iter) -
            std::to_address(prefetched.first.cbegin())) +
           prefetched.second;
  } else {
    return iter->second;
  }
}

template <GeneralShardBased Shard, bool Fwd>
GeneralSealedDSConstIterator<Shard, Fwd>::Block
GeneralSealedDSConstIterator<Shard, Fwd>::Block::shard_front_block(
    ShardsVecIter shards_iter) {
  Block b;
  if constexpr (Fwd) {
    if constexpr (kContiguous) {
      b.prefetched = shards_iter->run(&Shard::get_front_block, 1);
    } else {
      b.prefetched = shards_iter->run(&Shard::get_front_block_with_iters, 1);
    }
  } else {
    if constexpr (kContiguous) {
      b.prefetched = shards_iter->run(&Shard::get_rfront_block, 1);
    } else {
      b.prefetched = shards_iter->run(&Shard::get_rfront_block_with_iters, 1);
    }
  }
  return b;
}

template <GeneralShardBased Shard, bool Fwd>
GeneralSealedDSConstIterator<Shard, Fwd>::Block
GeneralSealedDSConstIterator<Shard, Fwd>::Block::shard_back_block(
    ShardsVecIter shards_iter) {
  Block b;
  if constexpr (Fwd) {
    if constexpr (kContiguous) {
      b.prefetched = shards_iter->run(&Shard::get_back_block, 1);
    } else {
      b.prefetched = shards_iter->run(&Shard::get_back_block_with_iters, 1);
    }
  } else {
    if constexpr (kContiguous) {
      b.prefetched = shards_iter->run(&Shard::get_rback_block, 1);
    } else {
      b.prefetched = shards_iter->run(&Shard::get_rback_block_with_iters, 1);
    }
  }
  return b;
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::Block
GeneralSealedDSConstIterator<Shard, Fwd>::Block::shard_end_block(
    ShardsVecIter shards_iter) {
  return Block();
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::GeneralSealedDSConstIterator()
    : block_iter_(block_.cbegin()),
      prefetched_next_blocks_(kMaxNumInflightPrefetches),
      prefetched_prev_blocks_(kMaxNumInflightPrefetches) {}

template <GeneralShardBased Shard, bool Fwd>
GeneralSealedDSConstIterator<Shard, Fwd>::GeneralSealedDSConstIterator(
    std::shared_ptr<ShardsVec> shards, bool is_begin)
    : shards_(shards),
      prefetched_next_blocks_(kMaxNumInflightPrefetches),
      prefetched_prev_blocks_(kMaxNumInflightPrefetches) {
  if constexpr (Fwd) {
    shards_iter_ = is_begin ? shards->begin() : shards->end();
  } else {
    shards_iter_ = is_begin ? shards->rbegin() : shards->rend();
  }
  block_ = (is_begin && !shards->empty())
               ? Block::shard_front_block(shards_iter_)
               : Block::shard_end_block(shards_iter_);
  update_prefetch_cnt(block_);
  block_iter_ = is_begin ? block_.cbegin() : block_.cend();
}

template <GeneralShardBased Shard, bool Fwd>
GeneralSealedDSConstIterator<Shard, Fwd>::GeneralSealedDSConstIterator(
    std::shared_ptr<ShardsVec> shards, ShardsVecIter shards_iter, IterVal val,
    ContainerIter iter)
    : shards_(shards),
      shards_iter_(shards_iter),
      block_(std::move(val), iter),
      block_iter_(block_.cbegin()),
      prefetched_next_blocks_(kMaxNumInflightPrefetches),
      prefetched_prev_blocks_(kMaxNumInflightPrefetches) {
  update_prefetch_cnt(block_);
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::GeneralSealedDSConstIterator(
    const GeneralSealedDSConstIterator &o)
    : prefetched_next_blocks_(kMaxNumInflightPrefetches),
      prefetched_prev_blocks_(kMaxNumInflightPrefetches) {
  *this = o;
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd> &
GeneralSealedDSConstIterator<Shard, Fwd>::operator=(
    const GeneralSealedDSConstIterator &o) {
  shards_ = o.shards_;
  shards_iter_ = o.shards_iter_;
  block_ = o.block_;
  block_iter_ = block_.cbegin() + (o.block_iter_ - o.block_.cbegin());
  prefetch_cnt_per_thread_ = o.prefetch_cnt_per_thread_;
  return *this;
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::GeneralSealedDSConstIterator(
    GeneralSealedDSConstIterator &&o) noexcept
    : prefetched_next_blocks_(kMaxNumInflightPrefetches),
      prefetched_prev_blocks_(kMaxNumInflightPrefetches) {
  *this = std::move(o);
}

template <GeneralShardBased Shard, bool Fwd>
GeneralSealedDSConstIterator<Shard, Fwd> &
GeneralSealedDSConstIterator<Shard, Fwd>::operator=(
    GeneralSealedDSConstIterator &&o) noexcept {
  shards_ = std::move(o.shards_);
  shards_iter_ = std::move(o.shards_iter_);
  block_ = std::move(o.block_);
  block_iter_ = std::move(o.block_iter_);
  prefetched_next_blocks_ = std::move(o.prefetched_next_blocks_);
  prefetched_prev_blocks_ = std::move(o.prefetched_prev_blocks_);
  for (auto &v : o.prefetched_next_blocks_) {
    unwrap_block_variant(&v);
  }
  for (auto &v : o.prefetched_prev_blocks_) {
    unwrap_block_variant(&v);
  }
  prefetch_executor_ = std::move(o.prefetch_executor_);
  prefetch_seq_ = o.prefetch_seq_;
  prefetch_cnt_per_thread_ = o.prefetch_cnt_per_thread_;

  return *this;
}

template <GeneralShardBased Shard, bool Fwd>
GeneralSealedDSConstIterator<Shard, Fwd>
GeneralSealedDSConstIterator<Shard, Fwd>::deep_copy() {
  GeneralSealedDSConstIterator it;

  if (shards_) {
    it.shards_.reset(new ShardsVec());
    *it.shards_ = *shards_;
    it.shards_iter_ =
        it.shards_vec_begin() + (shards_iter_ - shards_vec_begin());
  }
  it.block_ = block_;
  it.block_iter_ = it.block_.cbegin() + (block_iter_ - block_.cbegin());
  it.prefetch_cnt_per_thread_ = prefetch_cnt_per_thread_;

  return it;
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::ShardsVecIter
GeneralSealedDSConstIterator<Shard, Fwd>::shards_vec_begin() const {
  if constexpr (Fwd) {
    return shards_->begin();
  } else {
    return shards_->rbegin();
  }
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::ShardsVecIter
GeneralSealedDSConstIterator<Shard, Fwd>::shards_vec_end() const {
  if constexpr (Fwd) {
    return shards_->end();
  } else {
    return shards_->rend();
  }
}

template <GeneralShardBased Shard, bool Fwd>
[[gnu::always_inline]] inline auto
GeneralSealedDSConstIterator<Shard, Fwd>::to_gid() const {
  return block_.to_gid(block_iter_);
}

template <GeneralShardBased Shard, bool Fwd>
[[gnu::always_inline]] inline bool
GeneralSealedDSConstIterator<Shard, Fwd>::operator==(
    const GeneralSealedDSConstIterator &o) const {
  return to_gid() == o.to_gid();
}

template <GeneralShardBased Shard, bool Fwd>
inline GeneralSealedDSConstIterator<Shard, Fwd>::Block
GeneralSealedDSConstIterator<Shard, Fwd>::unwrap_block_variant(
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

template <GeneralShardBased Shard, bool Fwd>
GeneralSealedDSConstIterator<Shard, Fwd>::Block::Prefetched
GeneralSealedDSConstIterator<Shard, Fwd>::prefetch_next_block(
    Shard *shard, ContainerIter *container_iter, uint32_t cnt) {
  typename Block::Prefetched prefetched;

  if constexpr (kContiguous) {
    if constexpr (Fwd) {
      prefetched.first = shard->get_next_block(*container_iter, cnt);
    } else {
      prefetched.first = shard->get_next_rblock(*container_iter, cnt);
    }
    prefetched.second = *container_iter + 1;
    *container_iter += prefetched.first.size();
  } else {
    if constexpr (Fwd) {
      prefetched = shard->get_next_block_with_iters(*container_iter, cnt);
    } else {
      prefetched = shard->get_next_rblock_with_iters(*container_iter, cnt);
    }
    if (!prefetched.empty()) {
      *container_iter = prefetched.back().second;
    }
  }
  return prefetched;
}

template <GeneralShardBased Shard, bool Fwd>
GeneralSealedDSConstIterator<Shard, Fwd>::Block::Prefetched
GeneralSealedDSConstIterator<Shard, Fwd>::prefetch_prev_block(
    Shard *shard, ContainerIter *container_iter, uint32_t cnt) {
  typename Block::Prefetched prefetched;

  if constexpr (kContiguous) {
    if constexpr (Fwd) {
      prefetched.first = shard->get_prev_block(*container_iter, cnt);
    } else {
      prefetched.first = shard->get_prev_rblock(*container_iter, cnt);
    }
    *container_iter -= prefetched.first.size();
    prefetched.second = *container_iter;
  } else {
    if constexpr (Fwd) {
      prefetched = shard->get_prev_block_with_iters(*container_iter, cnt);
    } else {
      prefetched = shard->get_prev_rblock_with_iters(*container_iter, cnt);
    }
    if (!prefetched.empty()) {
      *container_iter = prefetched.front().second;
    }
  }
  return prefetched;
}

template <GeneralShardBased Shard, bool Fwd>
void GeneralSealedDSConstIterator<Shard, Fwd>::allocate_prefetch_executor() {
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
              return prefetch_next_block(states->first, &states->second,
                                         req.cnt);
            }
            BUG();
          } else {
            if constexpr (PreDecrementable<ContainerIter>) {
              return prefetch_prev_block(states->first, &states->second,
                                         req.cnt);
            }
            BUG();
          }
        },
        kMaxNumInflightPrefetches);
  });
}

template <GeneralShardBased Shard, bool Fwd>
Future<typename GeneralSealedDSConstIterator<Shard, Fwd>::Block>
GeneralSealedDSConstIterator<Shard, Fwd>::submit_prefetch_req(
    PrefetchReq prefetch_req) {
  uint32_t seq;

  if (prefetch_req.next) {
    seq = prefetch_seq_++;
  } else {
    seq = -prefetch_seq_--;
  }

  return nu::async([&, seq, prefetch_req = std::move(prefetch_req)] {
    auto block = Block(prefetch_executor_.run(
        +[](RobExecutor<PrefetchReq, typename Block::Prefetched> &rob_executor,
            uint32_t seq, PrefetchReq prefetch_req) {
          return rob_executor.submit(seq, std::move(prefetch_req));
        },
        seq, prefetch_req));
    update_prefetch_cnt(block);
    return block;
  });
}

template <GeneralShardBased Shard, bool Fwd>
[[gnu::always_inline]] inline GeneralSealedDSConstIterator<Shard, Fwd> &
GeneralSealedDSConstIterator<Shard, Fwd>::operator++()
  requires PreIncrementable<ContainerIter>
{
  if (unlikely(++block_iter_ == block_.cend())) {
    inc_slow_path();
  }
  return *this;
}

template <GeneralShardBased Shard, bool Fwd>
void GeneralSealedDSConstIterator<Shard, Fwd>::inc_slow_path() {
  if (unlikely(!prefetch_executor_)) {
    allocate_prefetch_executor();
    prefetch_seq_ = -1;
  }

  if (unlikely(prefetch_seq_ < 0)) {
    prefetch_seq_ = 0;
    prefetched_next_blocks_.push_back(submit_prefetch_req(PrefetchReq{
        block_.get_back_container_iter(), true, prefetch_cnt_per_thread_}));
    while (prefetched_next_blocks_.size() < kMaxNumInflightPrefetches) {
      prefetched_next_blocks_.push_back(submit_prefetch_req(
          PrefetchReq{std::nullopt, true, prefetch_cnt_per_thread_}));
    }
  }

  prefetched_prev_blocks_.push_back(std::move(block_));
  block_ = unwrap_block_variant(&prefetched_next_blocks_.front());
  prefetched_next_blocks_.pop_front();
  prefetched_next_blocks_.push_back(submit_prefetch_req(
      PrefetchReq{std::nullopt, true, prefetch_cnt_per_thread_}));

  if (unlikely(!block_)) {
    prefetched_next_blocks_.clear();
    prefetched_prev_blocks_.clear();
    prefetch_executor_.reset();
    if (likely(++shards_iter_ != shards_vec_end())) {
      block_ = Block::shard_front_block(shards_iter_);
      update_prefetch_cnt(block_);
    } else {
      block_ = Block();
    }
  }

  block_iter_ = block_.cbegin();
}

template <GeneralShardBased Shard, bool Fwd>
[[gnu::always_inline]] inline GeneralSealedDSConstIterator<Shard, Fwd> &
GeneralSealedDSConstIterator<Shard, Fwd>::operator--()
  requires PreDecrementable<ContainerIter>
{
  if (unlikely(block_iter_ == block_.cbegin())) {
    dec_slow_path();
  } else {
    block_iter_--;
  }
  return *this;
}

template <GeneralShardBased Shard, bool Fwd>
void GeneralSealedDSConstIterator<Shard, Fwd>::dec_slow_path() {
  if (unlikely(shards_iter_ == shards_vec_end())) {
    goto go_prev_shard;
  }

  if (unlikely(!prefetch_executor_)) {
    allocate_prefetch_executor();
    prefetch_seq_ = 1;
  }

  if (unlikely(prefetch_seq_ > 0)) {
    prefetch_seq_ = 0;
    prefetched_prev_blocks_.push_front(submit_prefetch_req(PrefetchReq{
        block_.get_front_container_iter(), false, prefetch_cnt_per_thread_}));
    while (prefetched_prev_blocks_.size() < kMaxNumInflightPrefetches) {
      prefetched_prev_blocks_.push_front(submit_prefetch_req(
          PrefetchReq{std::nullopt, false, prefetch_cnt_per_thread_}));
    }
  }

  prefetched_next_blocks_.push_front(std::move(block_));
  block_ = unwrap_block_variant(&prefetched_prev_blocks_.back());
  prefetched_prev_blocks_.pop_back();
  prefetched_prev_blocks_.push_front(submit_prefetch_req(
      PrefetchReq{std::nullopt, false, prefetch_cnt_per_thread_}));

go_prev_shard:
  if (unlikely(!block_)) {
    prefetched_prev_blocks_.clear();
    prefetched_next_blocks_.clear();
    prefetch_executor_.reset();
    BUG_ON(shards_iter_-- == shards_vec_begin());
    block_ = Block::shard_back_block(shards_iter_);
    update_prefetch_cnt(block_);
  }

  block_iter_ = --block_.cend();
}

template <GeneralShardBased Shard, bool Fwd>
void GeneralSealedDSConstIterator<Shard, Fwd>::update_prefetch_cnt(
    const Block &block) {
  if (block) {
    auto item_size = block.size_bytes() / block.size();
    prefetch_cnt_per_thread_ =
        std::max(1UL, kPrefetchBytesPerThread / item_size);
  }
}

template <GeneralShardBased Shard, bool Fwd>
[[gnu::always_inline]] inline const GeneralSealedDSConstIterator<Shard,
                                                                 Fwd>::IterVal &
GeneralSealedDSConstIterator<Shard, Fwd>::operator*() const {
  if constexpr (kContiguous) {
    return *block_iter_;
  } else {
    return block_iter_->first;
  }
}

template <GeneralShardBased Shard, bool Fwd>
[[gnu::always_inline]] inline const GeneralSealedDSConstIterator<Shard,
                                                                 Fwd>::IterVal *
GeneralSealedDSConstIterator<Shard, Fwd>::operator->() const {
  if constexpr (kContiguous) {
    return std::to_address(block_iter_);
  } else {
    return &block_iter_->first;
  }
}

template <GeneralShardBased Shard, bool Fwd>
[[gnu::always_inline]] inline GeneralSealedDSConstIterator<Shard,
                                                           Fwd>::IterVal &&
GeneralSealedDSConstIterator<Shard, Fwd>::move_deref() {
  if constexpr (kContiguous) {
    return std::move(const_cast<IterVal &>(*std::to_address(block_iter_)));
  } else {
    return std::move(const_cast<IterVal &>(block_iter_->first));
  }
}

template <GeneralShardBased Shard, bool Fwd>
template <class Archive>
inline void GeneralSealedDSConstIterator<Shard, Fwd>::save(Archive &ar) const {
  int64_t shards_offset = shards_ ? shards_iter_ - shards_vec_begin() : -1;
  uint64_t block_offset = block_iter_ - block_.cbegin();

  if (shards_) {
    ar(shards_offset, *shards_, block_.prefetched, block_offset,
       decltype(prefetch_executor_)(), prefetch_seq_, prefetch_cnt_per_thread_);
  } else {
    ar(shards_offset);
  }
}

template <GeneralShardBased Shard, bool Fwd>
template <class Archive>
inline void GeneralSealedDSConstIterator<Shard, Fwd>::save_move(Archive &ar) {
  int64_t shards_offset = shards_ ? shards_iter_ - shards_vec_begin() : -1;
  uint64_t block_offset = block_iter_ - block_.cbegin();

  if (shards_) {
    ar(shards_offset, std::move(*shards_), block_.prefetched, block_offset,
       std::move(prefetch_executor_), prefetch_seq_, prefetch_cnt_per_thread_);
  } else {
    ar(shards_offset);
  }
}

template <GeneralShardBased Shard, bool Fwd>
template <class Archive>
inline void GeneralSealedDSConstIterator<Shard, Fwd>::load(Archive &ar) {
  int64_t shards_offset;
  uint64_t block_offset;

  ar(shards_offset);
  if (shards_offset != -1) {
    shards_.reset(new ShardsVec());
    ar(*shards_, block_.prefetched, block_offset, prefetch_executor_,
       prefetch_seq_, prefetch_cnt_per_thread_);
    shards_iter_ = shards_vec_begin() + shards_offset;
    block_iter_ = block_.cbegin() + block_offset;
  }
}

template <ShardedDataStructureBased T>
inline SealedDS<T>::SealedDS(T &&t) : t_(std::move(t)) {
  t_.seal();

  auto all_shards_info = t_.get_all_shards_info();
  shards_ = std::make_shared<ShardsVec>();
  prefix_sum_sizes_.emplace_back(0);
  for (auto &[k, size, shard] : all_shards_info) {
    if (size) {
      keys_.emplace_back(k);
      shards_->emplace_back(shard);
      prefix_sum_sizes_.emplace_back(prefix_sum_sizes_.back() + size);
    }
  }

  if constexpr (ConstIterable<typename T::Shard>) {
    cbegin_ = ConstIterator(shards_, true);
    cend_ = ConstIterator(shards_, false);
  }
  if constexpr (ConstReverseIterable<typename T::Shard>) {
    crbegin_ = ConstReverseIterator(shards_, true);
    crend_ = ConstReverseIterator(shards_, false);
  }
}

template <ShardedDataStructureBased T>
inline SealedDS<T>::~SealedDS() {
  if (shards_) {
    t_.unseal();
  }
}

template <ShardedDataStructureBased T>
inline T &&SealedDS<T>::unseal() {
  t_.unseal();
  shards_.reset();
  return std::move(t_);
}

template <ShardedDataStructureBased T>
inline const SealedDS<T>::ConstIterator &SealedDS<T>::begin() const
  requires ConstIterable<typename T::Shard>
{
  return cbegin_;
}

template <ShardedDataStructureBased T>
inline const SealedDS<T>::ConstIterator &SealedDS<T>::cbegin() const
  requires ConstIterable<typename T::Shard>
{
  return cbegin_;
}

template <ShardedDataStructureBased T>
inline const SealedDS<T>::ConstIterator &SealedDS<T>::end() const
  requires ConstIterable<typename T::Shard>
{
  return cend_;
}

template <ShardedDataStructureBased T>
inline const SealedDS<T>::ConstIterator &SealedDS<T>::cend() const
  requires ConstIterable<typename T::Shard>
{
  return cend_;
}

template <ShardedDataStructureBased T>
inline const SealedDS<T>::ConstReverseIterator &SealedDS<T>::rbegin() const
  requires ConstReverseIterable<typename T::Shard>
{
  return crbegin_;
}

template <ShardedDataStructureBased T>
inline const SealedDS<T>::ConstReverseIterator &SealedDS<T>::crbegin() const
  requires ConstReverseIterable<typename T::Shard>
{
  return crbegin_;
}

template <ShardedDataStructureBased T>
inline const SealedDS<T>::ConstReverseIterator &SealedDS<T>::rend() const
  requires ConstReverseIterable<typename T::Shard>
{
  return crend_;
}

template <ShardedDataStructureBased T>
inline const SealedDS<T>::ConstReverseIterator &SealedDS<T>::crend() const
  requires ConstReverseIterable<typename T::Shard>
{
  return crend_;
}

template <ShardedDataStructureBased T>
inline bool SealedDS<T>::empty() const {
  return !size();
}

template <ShardedDataStructureBased T>
inline std::size_t SealedDS<T>::size() const {
  return prefix_sum_sizes_.back();
}

template <ShardedDataStructureBased T>
inline SealedDS<T>::ConstIterator SealedDS<T>::find_iter(T::Key k) const
  requires FindAble<typename T::ContainerImpl>
{
  return const_cast<SealedDS *>(this)->__find_iter(std::move(k));
}

template <ShardedDataStructureBased T>
std::optional<typename T::IterVal> SealedDS<T>::find_data_by_order(
    std::size_t order)
  requires FindAbleByOrder<typename T::ContainerImpl>
{
  auto iter = std::ranges::upper_bound(prefix_sum_sizes_, order);

  if (iter == prefix_sum_sizes_.end()) {
    return std::nullopt;
  }
  BUG_ON(iter == prefix_sum_sizes_.begin());
  --iter;
  auto shard_idx = iter - prefix_sum_sizes_.begin();
  auto &shard = (*shards_)[shard_idx];
  auto local_order = order - *iter;
  return shard.run(&Shard::find_data_by_order, local_order);
}

template <ShardedDataStructureBased T>
inline SealedDS<T>::ShardsVec::iterator SealedDS<T>::search_shard(T::Key k) {
  auto idx =
      std::upper_bound(keys_.begin(), keys_.end(), k) - keys_.begin() - 1;
  return shards_->begin() + idx;
}

template <ShardedDataStructureBased T>
inline SealedDS<T>::ConstIterator SealedDS<T>::__find_iter(T::Key k)
  requires FindAble<typename T::ContainerImpl>
{
  auto shard_iter = search_shard(k);
  auto p = shard_iter->run(&Shard::find, k);
  return ConstIterator(shards_, shard_iter, std::move(p.first),
                       std::move(p.second));
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
