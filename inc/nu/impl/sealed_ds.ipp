#include <algorithm>

namespace nu {

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::Block() {
  static ContainerIter iter;
  prefetched.reserve(1);
  prefetched.data()->second = iter;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::Block(
    ShardsVecIter shards_vec_iter, Val v, ContainerIter container_iter)
    : shards_iter(shards_vec_iter),
      prefetched{std::make_pair(std::move(v), container_iter)} {}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::Block(const Block &o) {
  *this = o;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block &
GeneralSealedDSConstIterator<T, Fwd>::Block::operator=(const Block &o) {
  shards_iter = o.shards_iter;
  prefetched = o.prefetched;
  return *this;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::Block(Block &&o) {
  *this = std::move(o);
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block &
GeneralSealedDSConstIterator<T, Fwd>::Block::operator=(Block &&o) {
  shards_iter = std::move(o.shards_iter);
  prefetched = std::move(o.prefetched);
  return *this;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::operator bool() const {
  return !prefetched.empty();
}

template <typename T, bool Fwd>
bool GeneralSealedDSConstIterator<T, Fwd>::Block::empty() const {
  return prefetched.empty();
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::ConstIterator
GeneralSealedDSConstIterator<T, Fwd>::Block::cbegin() const {
  return prefetched.cbegin();
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::ConstReverseIterator
GeneralSealedDSConstIterator<T, Fwd>::Block::crbegin() const {
  return prefetched.crbegin();
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::ConstIterator
GeneralSealedDSConstIterator<T, Fwd>::Block::cend() const {
  return prefetched.cend();
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::ShardsVecIter
GeneralSealedDSConstIterator<T, Fwd>::Block::get_shards_iter() const {
  return shards_iter;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::Block::shard_front_block(
    ShardsVecIter shards_vec_iter) {
  Block b;
  b.shards_iter = shards_vec_iter;
  if constexpr (Fwd) {
    b.prefetched = b.shards_iter->run(&Shard::get_front_block, kSize);
  } else {
    b.prefetched = b.shards_iter->run(&Shard::get_rfront_block, kSize);
  }
  return b;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::Block::shard_back_block(
    ShardsVecIter shards_vec_iter) {
  Block b;
  b.shards_iter = shards_vec_iter;
  if constexpr (Fwd) {
    b.prefetched = b.shards_iter->run(&Shard::get_back_block, kSize);
  } else {
    b.prefetched = b.shards_iter->run(&Shard::get_rback_block, kSize);
  }
  return b;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::Block::shard_end_block(
    ShardsVecIter shards_vec_iter) {
  Block b;
  b.shards_iter = shards_vec_iter;
  return b;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::Block::next_block() const {
  Block b;
  b.shards_iter = shards_iter;
  if constexpr (Fwd) {
    b.prefetched = shards_iter->run(&Shard::get_block_forward,
                                    prefetched.back().second, kSize);
  } else {
    b.prefetched = shards_iter->run(&Shard::get_rblock_forward,
                                    prefetched.back().second, kSize);
  }
  return b;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::Block::prev_block() const {
  Block b;
  b.shards_iter = shards_iter;
  if constexpr (Fwd) {
    b.prefetched = shards_iter->run(&Shard::get_block_backward,
                                    prefetched.front().second, kSize);
  } else {
    b.prefetched = shards_iter->run(&Shard::get_rblock_backward,
                                    prefetched.front().second, kSize);
  }
  return b;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator() {
  block_iter_ = block_.cbegin();
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator(
    std::shared_ptr<ShardsVec> &shards, bool is_begin)
    : shards_(shards) {
  ShardsVecIter shards_iter;
  if constexpr (Fwd) {
    shards_iter = is_begin ? shards->begin() : shards->end();
  } else {
    shards_iter = is_begin ? shards->rbegin() : shards->rend();
  }
  block_ = is_begin ? Block::shard_front_block(shards_iter)
                    : Block::shard_end_block(shards_iter);
  block_iter_ = is_begin ? block_.cbegin() : block_.cend();
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator(
    std::shared_ptr<ShardsVec> &shards, ShardsVecIter shards_iter, Val val,
    ContainerIter iter)
    : shards_(shards), block_(shards_iter, std::move(val), iter) {
  block_iter_ = block_.cbegin();
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator(
    const GeneralSealedDSConstIterator &o) {
  *this = o;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>
    &GeneralSealedDSConstIterator<T, Fwd>::operator=(
        const GeneralSealedDSConstIterator &o) {
  shards_ = o.shards_;
  block_ = o.block_;
  block_iter_ = block_.cbegin() + (o.block_iter_ - o.block_.cbegin());
  return *this;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator(
    GeneralSealedDSConstIterator &&o) noexcept {
  *this = std::move(o);
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>
    &GeneralSealedDSConstIterator<T, Fwd>::operator=(
        GeneralSealedDSConstIterator &&o) noexcept {
  shards_ = std::move(o.shards_);
  block_ = std::move(o.block_);
  block_iter_ = std::move(o.block_iter_);
  return *this;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::ShardsVecIter
GeneralSealedDSConstIterator<T, Fwd>::shards_vec_begin() const {
  if constexpr (Fwd) {
    return shards_->begin();
  } else {
    return shards_->rbegin();
  }
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::ShardsVecIter
GeneralSealedDSConstIterator<T, Fwd>::shards_vec_end() const {
  if constexpr (Fwd) {
    return shards_->end();
  } else {
    return shards_->rend();
  }
}

template <typename T, bool Fwd>
bool GeneralSealedDSConstIterator<T, Fwd>::operator==(
    const GeneralSealedDSConstIterator &o) const {
  return block_iter_->second == o.block_iter_->second;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::get_next_block(const Block &block) {
  Block new_block = block.next_block();
  if (unlikely(new_block.empty())) {
    auto new_shards_iter = new_block.get_shards_iter();
    if (unlikely(++new_shards_iter == shards_vec_end())) {
      return Block();
    } else {
      return Block::shard_front_block(new_shards_iter);
    }
  }
  return new_block;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::get_prev_block(const Block &block) {
  Block new_block = block ? block.prev_block() : block;
  if (unlikely(new_block.empty())) {
    auto new_shards_iter = new_block.get_shards_iter();
    if (unlikely(new_shards_iter-- == shards_vec_begin())) {
      return Block();
    } else {
      return Block::shard_back_block(new_shards_iter);
    }
  }
  return new_block;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>
    &GeneralSealedDSConstIterator<T, Fwd>::operator++() {
  if (unlikely(++block_iter_ == block_.cend())) {
    if (likely(prefetched_next_block_)) {
      block_ = std::move(prefetched_next_block_.get());
    } else {
      block_ = get_next_block(block_);
    }
    if (block_) {
      prefetched_next_block_ =
          nu::async([&] { return get_next_block(block_); });
    }
    block_iter_ = block_.cbegin();
  }
  return *this;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>
    &GeneralSealedDSConstIterator<T, Fwd>::operator--() {
  if (unlikely(block_iter_ == block_.cbegin())) {
    if (likely(prefetched_prev_block_)) {
      block_ = std::move(prefetched_prev_block_.get());
    } else {
      block_ = get_prev_block(block_);
    }
    if (block_) {
      prefetched_prev_block_ =
          nu::async([&] { return get_prev_block(block_); });
    }
    block_iter_ = --block_.cend();
  } else {
    block_iter_--;
  }
  return *this;
}

template <typename T, bool Fwd>
const GeneralSealedDSConstIterator<T, Fwd>::Val &
GeneralSealedDSConstIterator<T, Fwd>::operator*() const {
  return block_iter_->first;
}

template <typename T, bool Fwd>
const GeneralSealedDSConstIterator<T, Fwd>::Val *
GeneralSealedDSConstIterator<T, Fwd>::operator->() const {
  return &block_iter_->first;
}

template <typename T, bool Fwd>
template <class Archive>
void GeneralSealedDSConstIterator<T, Fwd>::save(Archive &ar) const {
  uint64_t shards_offset = block_.shards_iter - shards_vec_begin();
  uint64_t block_offset = block_iter_ - block_.cbegin();
  ar(shards_, shards_offset, block_.begin_iter, block_.end_iter, block_.data,
     block_offset);
}

template <typename T, bool Fwd>
template <class Archive>
void GeneralSealedDSConstIterator<T, Fwd>::load(Archive &ar) {
  uint64_t shards_offset, block_offset;
  ar(shards_, shards_offset, block_.begin_iter, block_.end_iter, block_.data,
     block_offset);
  block_.shards_iter = shards_vec_begin() + shards_offset;
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
SealedDS<T>::~SealedDS() {
  if (shards_) {
    t_.unseal();
  }
}

template <typename T>
T &&SealedDS<T>::unseal() {
  t_.unseal();
  shards_.reset();
  return std::move(t_);
}

template <typename T>
SealedDS<T>::ConstIterator SealedDS<T>::begin() const
    requires ConstIterable<typename T::Shard> {
  return cbegin_;
}

template <typename T>
SealedDS<T>::ConstIterator SealedDS<T>::cbegin() const
    requires ConstIterable<typename T::Shard> {
  return cbegin_;
}

template <typename T>
SealedDS<T>::ConstIterator SealedDS<T>::end() const
    requires ConstIterable<typename T::Shard> {
  return cend_;
}

template <typename T>
SealedDS<T>::ConstIterator SealedDS<T>::cend() const
    requires ConstIterable<typename T::Shard> {
  return cend_;
}

template <typename T>
SealedDS<T>::ConstReverseIterator SealedDS<T>::rbegin() const
    requires ConstReverseIterable<typename T::Shard> {
  return crbegin_;
}

template <typename T>
SealedDS<T>::ConstReverseIterator SealedDS<T>::crbegin() const
    requires ConstReverseIterable<typename T::Shard> {
  return crbegin_;
}

template <typename T>
SealedDS<T>::ConstReverseIterator SealedDS<T>::rend() const
    requires ConstReverseIterable<typename T::Shard> {
  return crend_;
}

template <typename T>
SealedDS<T>::ConstReverseIterator SealedDS<T>::crend() const
    requires ConstReverseIterable<typename T::Shard> {
  return crend_;
}

template <typename T>
bool SealedDS<T>::empty() const {
  return !size();
}

template <typename T>
std::size_t SealedDS<T>::size() const {
  return const_cast<SealedDS *>(this)->__size();
}

template <typename T>
SealedDS<T>::ConstIterator SealedDS<T>::find_iter(T::Key k) const {
  return const_cast<SealedDS *>(this)->__find_iter(std::move(k));
}

template <typename T>
SealedDS<T>::ShardsVec::iterator SealedDS<T>::search_shard(T::Key k) {
  auto idx =
      std::upper_bound(keys_.begin(), keys_.end(), k) - keys_.begin() - 1;
  return shards_->begin() + idx;
}

template <typename T>
SealedDS<T>::ConstIterator SealedDS<T>::__find_iter(T::Key k) {
  auto shard_iter = search_shard(k);
  auto tuple = shard_iter->run(&Shard::find, k);
  BUG_ON(!std::get<0>(tuple));
  return ConstIterator(shards_, shard_iter, std::move(std::get<1>(tuple)),
                       std::move(std::get<2>(tuple)));
}

template <typename T>
std::size_t SealedDS<T>::__size() {
  if (unlikely(!size_)) {
    size_ = t_.size();
  }
  return *size_;
}

template <typename T>
SealedDS<T> to_sealed_ds(T &&t) {
  return SealedDS<T>(std::move(t));
}

template <typename T>
T to_unsealed_ds(SealedDS<T> &&sealed) {
  return sealed.unseal();
}

}  // namespace nu
