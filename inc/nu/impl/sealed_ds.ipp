#include <algorithm>

namespace nu {

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::Block() {}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::Block(
    ShardsVecIter shards_vec_iter, ContainerIter block_begin_iter)
    : shards_iter(shards_vec_iter), begin_iter(block_begin_iter) {
  if constexpr (Fwd) {
    std::tie(data, end_iter) =
        shards_iter->run(&Shard::get_block_forward, begin_iter, kSize);
  } else {
    std::tie(data, end_iter) =
        shards_iter->run(&Shard::get_rblock_forward, begin_iter, kSize);
  }
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::Block(const Block &o) {
  *this = o;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block &
GeneralSealedDSConstIterator<T, Fwd>::Block::operator=(const Block &o) {
  shards_iter = o.shards_iter;
  begin_iter = o.begin_iter;
  end_iter = o.end_iter;
  data = o.data;
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
  begin_iter = std::move(o.begin_iter);
  end_iter = std::move(o.end_iter);
  data = std::move(o.data);
  return *this;
}

template <typename T, bool Fwd>
bool GeneralSealedDSConstIterator<T, Fwd>::Block::operator==(
    const Block &o) const {
  return begin_iter == o.begin_iter;
}

template <typename T, bool Fwd>
bool GeneralSealedDSConstIterator<T, Fwd>::Block::empty() const {
  return data.empty();
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::ConstIterator
GeneralSealedDSConstIterator<T, Fwd>::Block::cbegin() const {
  return data.cbegin();
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block::ConstIterator
GeneralSealedDSConstIterator<T, Fwd>::Block::cend() const {
  return data.cend();
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::ShardsVecIter
GeneralSealedDSConstIterator<T, Fwd>::Block::get_shards_iter() const {
  return shards_iter;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::Block::shard_head_block(
    ShardsVecIter shards_vec_iter) {
  Block b;
  b.shards_iter = shards_vec_iter;
  if constexpr (Fwd) {
    b.begin_iter = b.shards_iter->run(&Shard::cbegin);
    std::tie(b.data, b.end_iter) =
        b.shards_iter->run(&Shard::get_block_forward, b.begin_iter, kSize);
  } else {
    b.begin_iter = b.shards_iter->run(&Shard::crbegin);
    std::tie(b.data, b.end_iter) =
        b.shards_iter->run(&Shard::get_rblock_forward, b.begin_iter, kSize);
  }
  return b;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::Block::shard_tail_block(
    ShardsVecIter shards_vec_iter) {
  Block b;
  b.shards_iter = shards_vec_iter;
  if constexpr (Fwd) {
    b.end_iter = b.shards_iter->run(&Shard::cend);
    std::tie(b.data, b.begin_iter) =
        b.shards_iter->run(&Shard::get_block_backward, b.end_iter, kSize);
  } else {
    b.end_iter = b.shards_iter->run(&Shard::crend);
    std::tie(b.data, b.begin_iter) =
        b.shards_iter->run(&Shard::get_rblock_backward, b.end_iter, kSize);
  }
  std::reverse(b.data.begin(), b.data.end());
  return b;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::Block::next_block() const {
  Block b;
  b.shards_iter = shards_iter;
  b.begin_iter = end_iter;
  if constexpr (Fwd) {
    std::tie(b.data, b.end_iter) =
        b.shards_iter->run(&Shard::get_block_forward, b.begin_iter, kSize);
  } else {
    std::tie(b.data, b.end_iter) =
        b.shards_iter->run(&Shard::get_rblock_forward, b.begin_iter, kSize);
  }
  return b;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::Block::prev_block() const {
  Block b;
  b.shards_iter = shards_iter;
  b.end_iter = begin_iter;
  if constexpr (Fwd) {
    std::tie(b.data, b.begin_iter) =
        shards_iter->run(&Shard::get_block_backward, b.end_iter, kSize);
  } else {
    std::tie(b.data, b.begin_iter) =
        b.shards_iter->run(&Shard::get_rblock_backward, b.end_iter, kSize);
  }
  std::reverse(b.data.begin(), b.data.end());
  return b;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator() {}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator(
    std::shared_ptr<ShardsVec> &shards, ShardsVecIter shards_vec_iter,
    ContainerIter block_begin_iter)
    : shards_(shards),
      block_(shards_vec_iter, block_begin_iter),
      block_iter_(block_.cbegin()) {}

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
  return block_ == o.block_ &&
         (block_iter_ - block_.cbegin() == o.block_iter_ - o.block_.cbegin());
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::get_next_block(const Block &block) {
  Block new_block = block.next_block();
  if (unlikely(new_block.empty())) {
    auto new_shards_iter = ++new_block.get_shards_iter();
    if (likely(new_shards_iter != shards_vec_end())) {
      return Block::shard_head_block(new_shards_iter);
    }
  }
  return new_block;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Block
GeneralSealedDSConstIterator<T, Fwd>::get_prev_block(const Block &block) {
  Block new_block = block.prev_block();
  if (unlikely(new_block.empty())) {
    auto new_shards_iter = new_block.get_shards_iter();
    if (likely(new_shards_iter != shards_vec_begin())) {
      --new_shards_iter;
      return Block::shard_tail_block(new_shards_iter);
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
    prefetched_next_block_ = nu::async([&] { return get_next_block(block_); });
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
    prefetched_prev_block_ = nu::async([&] { return get_prev_block(block_); });
    block_iter_ = --block_.cend();
  } else {
    block_iter_--;
  }
  return *this;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Val
GeneralSealedDSConstIterator<T, Fwd>::operator*() {
  return *block_iter_;
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
  shards_ = std::make_shared<ShardsVec>(t_.get_all_non_empty_shards());

  if (shards_->empty()) {
    if constexpr (ConstIterable<typename T::Shard>) {
      cbegin_ = ConstIterator();
      cend_ = cbegin_;
    }
    if constexpr (ConstReverseIterable<typename T::Shard>) {
      crbegin_ = ConstReverseIterator();
      crend_ = crbegin_;
    }
  } else {
    if constexpr (ConstIterable<typename T::Shard>) {
      cbegin_ = ConstIterator(shards_, shards_->begin(),
                              shards_->front().run(&T::Shard::cbegin));
      cend_ = ConstIterator(shards_, --shards_->end(),
                            shards_->back().run(&T::Shard::cend));
    }
    if constexpr (ConstReverseIterable<typename T::Shard>) {
      crbegin_ = ConstReverseIterator(shards_, shards_->rbegin(),
                                      shards_->back().run(&T::Shard::crbegin));
      crend_ = ConstReverseIterator(shards_, --shards_->rend(),
                                    shards_->front().run(&T::Shard::crend));
    }
  }
}

template <typename T>
SealedDS<T>::SealedDS(SealedDS &&o)
    : t_(std::move(o.t_)), shards_(std::move(o.shards_)) {}

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
std::size_t SealedDS<T>::size() const {
  return t_.size();
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
