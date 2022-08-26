namespace nu {

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator() {}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::GeneralSealedDSConstIterator(
    std::shared_ptr<ShardsVec> &shards, ShardsVecIter shards_vec_iter,
    ContainerIter container_iter)
    : shards_(shards),
      shards_vec_iter_(shards_vec_iter),
      container_iter_(container_iter) {}

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
  container_iter_ = o.container_iter_;
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
  shards_ = o.shards_;
  shards_vec_iter_ = std::move(o.shards_vec_iter_);
  container_iter_ = std::move(o.container_iter_);
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
  return container_iter_ == o.container_iter_;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>
    &GeneralSealedDSConstIterator<T, Fwd>::operator++() {
  std::optional<ContainerIter> optional_new_container_iter;

  if constexpr (Fwd) {
    optional_new_container_iter =
        shards_vec_iter_->run(&Shard::inc_iter, container_iter_);
  } else {
    optional_new_container_iter =
        shards_vec_iter_->run(&Shard::inc_riter, container_iter_);
  }

  if (unlikely(!optional_new_container_iter)) {
    if (unlikely(++shards_vec_iter_ == shards_vec_end())) {
      shards_vec_iter_--;
      return *this;
    }
    if constexpr (Fwd) {
      optional_new_container_iter = shards_vec_iter_->run(&Shard::cbegin);
    } else {
      optional_new_container_iter = shards_vec_iter_->run(&Shard::crbegin);
    }
  }

  container_iter_ = *optional_new_container_iter;
  return *this;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>
GeneralSealedDSConstIterator<T, Fwd>::operator++(int) {
  auto old = *this;
  ++*this;
  return old;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>
    &GeneralSealedDSConstIterator<T, Fwd>::operator--() {
  std::optional<ContainerIter> optional_new_container_iter;

  if constexpr (Fwd) {
    optional_new_container_iter =
        shards_vec_iter_->run(&Shard::dec_iter, container_iter_);
  } else {
    optional_new_container_iter =
        shards_vec_iter_->run(&Shard::dec_riter, container_iter_);
  }

  if (unlikely(!optional_new_container_iter)) {
    BUG_ON(shards_vec_iter_ == shards_vec_begin());
    shards_vec_iter_--;
    if constexpr (Fwd) {
      optional_new_container_iter = shards_vec_iter_->run(&Shard::clast);
    } else {
      optional_new_container_iter = shards_vec_iter_->run(&Shard::crlast);
    }
  }

  container_iter_ = *optional_new_container_iter;
  return *this;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>
GeneralSealedDSConstIterator<T, Fwd>::operator--(int) {
  auto old = *this;
  --*this;
  return old;
}

template <typename T, bool Fwd>
GeneralSealedDSConstIterator<T, Fwd>::Val
GeneralSealedDSConstIterator<T, Fwd>::operator*() {
  return shards_vec_iter_->run(
      +[](T::Shard &_, ContainerIter iter) { return *iter; }, container_iter_);
}

template <typename T, bool Fwd>
template <class Archive>
void GeneralSealedDSConstIterator<T, Fwd>::save(Archive &ar) const {
  uint64_t shards_vec_offset = shards_vec_iter_ - shards_vec_begin();
  ar(shards_, shards_vec_offset, container_iter_);
}

template <typename T, bool Fwd>
template <class Archive>
void GeneralSealedDSConstIterator<T, Fwd>::load(Archive &ar) {
  uint64_t shards_vec_offset;
  ar(shards_, shards_vec_offset, container_iter_);
  shards_vec_iter_ = shards_vec_begin() + shards_vec_offset;
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
