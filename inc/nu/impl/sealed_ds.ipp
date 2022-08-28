#include <algorithm>

namespace nu {

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator() {}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator(
    std::shared_ptr<ShardsVec> &shards, ShardsVecIter shards_vec_iter,
    ContainerIter block_begin_iter)
    : shards_(shards),
      shards_vec_iter_(shards_vec_iter),
      block_begin_iter_(block_begin_iter) {
  if constexpr (Fwd) {
    std::tie(block_data_, block_end_iter_) = shards_vec_iter_->run(
        &Shard::get_block_forward, block_begin_iter_, kBlockSize);
  } else {
    std::tie(block_data_, block_end_iter_) = shards_vec_iter_->run(
        &Shard::get_rblock_forward, block_begin_iter_, kBlockSize);
  }
  block_iter_ = block_data_.begin();
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
  shards_vec_iter_ = o.shards_vec_iter_;
  block_begin_iter_ = o.block_begin_iter_;
  block_end_iter_ = o.block_end_iter_;
  block_data_ = o.block_data_;
  block_iter_ = block_data_.begin() + (o.block_iter_ - o.block_data_.begin());
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
  shards_vec_iter_ = std::move(o.shards_vec_iter_);
  block_begin_iter_ = std::move(o.block_begin_iter_);
  block_end_iter_ = std::move(o.block_end_iter_);
  block_iter_ = std::move(o.block_iter_);
  block_data_ = std::move(o.block_data_);
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
  return block_begin_iter_ == o.block_begin_iter_ &&
         (block_iter_ - block_data_.begin() ==
          o.block_iter_ - o.block_data_.begin());
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>
    &GeneralSealedDSConstIterator<T, Fwd>::operator++() {
  if (unlikely(++block_iter_ == block_data_.end())) {
    block_begin_iter_ = block_end_iter_;
    if constexpr (Fwd) {
      std::tie(block_data_, block_end_iter_) = shards_vec_iter_->run(
          &Shard::get_block_forward, block_end_iter_, kBlockSize);
    } else {
      std::tie(block_data_, block_end_iter_) = shards_vec_iter_->run(
          &Shard::get_rblock_forward, block_end_iter_, kBlockSize);
    }
    if (unlikely(block_data_.empty())) {
      if (unlikely(++shards_vec_iter_ == shards_vec_end())) {
        shards_vec_iter_--;
        block_iter_ = block_data_.begin();
        return *this;
      }
      if constexpr (Fwd) {
        block_begin_iter_ = shards_vec_iter_->run(&Shard::cbegin);
        std::tie(block_data_, block_end_iter_) = shards_vec_iter_->run(
            &Shard::get_block_forward, block_begin_iter_, kBlockSize);
      } else {
        block_begin_iter_ = shards_vec_iter_->run(&Shard::crbegin);
        std::tie(block_data_, block_end_iter_) = shards_vec_iter_->run(
            &Shard::get_rblock_forward, block_begin_iter_, kBlockSize);
      }
    }
    block_iter_ = block_data_.begin();
  }
  return *this;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>
    &GeneralSealedDSConstIterator<T, Fwd>::operator--() {
  if (unlikely(block_iter_ == block_data_.begin())) {
    block_end_iter_ = block_begin_iter_;
    if constexpr (Fwd) {
      std::tie(block_data_, block_begin_iter_) = shards_vec_iter_->run(
          &Shard::get_block_backward, block_begin_iter_, kBlockSize);
    } else {
      std::tie(block_data_, block_begin_iter_) = shards_vec_iter_->run(
          &Shard::get_rblock_backward, block_begin_iter_, kBlockSize);
    }
    if (unlikely(block_data_.empty())) {
      BUG_ON(shards_vec_iter_ == shards_vec_begin());
      shards_vec_iter_--;
      if constexpr (Fwd) {
        block_end_iter_ = shards_vec_iter_->run(&Shard::cend);
        std::tie(block_data_, block_begin_iter_) = shards_vec_iter_->run(
            &Shard::get_block_backward, block_end_iter_, kBlockSize);
      } else {
        block_end_iter_ = shards_vec_iter_->run(&Shard::crend);
        std::tie(block_data_, block_begin_iter_) = shards_vec_iter_->run(
            &Shard::get_rblock_backward, block_end_iter_, kBlockSize);
      }
    }
    std::reverse(block_data_.begin(), block_data_.end());
    block_iter_ = --block_data_.end();
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
  uint64_t shards_vec_offset = shards_vec_iter_ - shards_vec_begin();
  uint64_t block_offset = block_iter_ - block_data_.begin();
  ar(shards_, shards_vec_offset, block_begin_iter_, block_end_iter_,
     block_offset, block_data_);
}

template <typename T, bool Fwd>
template <class Archive>
void GeneralSealedDSConstIterator<T, Fwd>::load(Archive &ar) {
  uint64_t shards_vec_offset, block_offset;
  ar(shards_, shards_vec_offset, block_begin_iter_, block_end_iter_,
     block_offset, block_data_);
  shards_vec_iter_ = shards_vec_begin() + shards_vec_offset;
  block_iter_ = block_data_.begin() + block_offset;
}

template <typename T>
SealedDS<T>::SealedDS(T &&t) : t_(std::move(t)) {
  t_.seal();
  shards_ = std::make_shared<ShardsVec>(t_.get_all_non_empty_shards());

  if (shards_->empty()) {
    cbegin_ = ConstIterator();
    cend_ = cbegin_;
    crbegin_ = ConstReverseIterator();
    crend_ = crbegin_;
  } else {
    cbegin_ = ConstIterator(shards_, shards_->begin(),
                            shards_->front().run(&T::Shard::cbegin));
    cend_ = ConstIterator(shards_, --shards_->end(),
                          shards_->back().run(&T::Shard::cend));
    crbegin_ = ConstReverseIterator(shards_, shards_->rbegin(),
                                    shards_->back().run(&T::Shard::crbegin));
    crend_ = ConstReverseIterator(shards_, --shards_->rend(),
                                  shards_->front().run(&T::Shard::crend));
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
SealedDS<T>::ConstIterator SealedDS<T>::cbegin() {
  return cbegin_;
}

template <typename T>
SealedDS<T>::ConstIterator SealedDS<T>::cend() {
  return cend_;
}

template <typename T>
SealedDS<T>::ConstReverseIterator SealedDS<T>::crbegin() {
  return crbegin_;
}

template <typename T>
SealedDS<T>::ConstReverseIterator SealedDS<T>::crend() {
  return crend_;
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
