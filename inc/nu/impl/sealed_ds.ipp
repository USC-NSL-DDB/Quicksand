namespace nu {

template <typename T>
SealedDSConstIterator<T>::SealedDSConstIterator() {}

template <typename T>
SealedDSConstIterator<T>::SealedDSConstIterator(
    std::shared_ptr<ShardsVec> &shards, ShardsVecIter shards_vec_iter,
    ContainerIter container_iter)
    : shards_(shards),
      shards_vec_iter_(shards_vec_iter),
      container_iter_(container_iter) {}

template <typename T>
SealedDSConstIterator<T>::SealedDSConstIterator(
    const SealedDSConstIterator &o) {
  *this = o;
}

template <typename T>
SealedDSConstIterator<T> &SealedDSConstIterator<T>::operator=(
    const SealedDSConstIterator &o) {
  shards_ = o.shards_;
  shards_vec_iter_ = o.shards_vec_iter_;
  container_iter_ = o.container_iter_;
  return *this;
}

template <typename T>
SealedDSConstIterator<T>::SealedDSConstIterator(
    SealedDSConstIterator &&o) noexcept {
  *this = std::move(o);
}

template <typename T>
SealedDSConstIterator<T> &SealedDSConstIterator<T>::operator=(
    SealedDSConstIterator &&o) noexcept {
  shards_ = o.shards_;
  shards_vec_iter_ = std::move(o.shards_vec_iter_);
  container_iter_ = std::move(o.container_iter_);
  return *this;
}

template <typename T>
bool SealedDSConstIterator<T>::operator==(
    const SealedDSConstIterator &o) const {
  if (likely(shards_ && o.shards_)) {
    // *this and o may have shards_ of different addresses but of the same data.
    return shards_vec_iter_ - shards_->begin() ==
               o.shards_vec_iter_ - o.shards_->begin() &&
           container_iter_ == o.container_iter_;
  } else {
    return !shards_ && !o.shards_;
  }
}

template <typename T>
SealedDSConstIterator<T> &SealedDSConstIterator<T>::operator++() {
  auto optional_new_container_iter =
      shards_vec_iter_->run(&Shard::inc_iter, container_iter_);
  if (unlikely(!optional_new_container_iter)) {
    if (unlikely(++shards_vec_iter_ == shards_->end())) {
      shards_vec_iter_--;
      return *this;
    }
    optional_new_container_iter = shards_vec_iter_->run(&Shard::cbegin);
  }

  container_iter_ = *optional_new_container_iter;
  return *this;
}

template <typename T>
SealedDSConstIterator<T> SealedDSConstIterator<T>::operator++(int) {
  auto old = *this;
  ++*this;
  return old;
}

template <typename T>
SealedDSConstIterator<T> &SealedDSConstIterator<T>::operator--() {
  auto optional_new_container_iter =
      shards_vec_iter_->run(&Shard::dec_iter, container_iter_);
  if (unlikely(!optional_new_container_iter)) {
    BUG_ON(shards_vec_iter_ == shards_->begin());
    shards_vec_iter_--;
    optional_new_container_iter = shards_vec_iter_->run(&Shard::clast);
  }

  container_iter_ = *optional_new_container_iter;
  return *this;
}

template <typename T>
SealedDSConstIterator<T> SealedDSConstIterator<T>::operator--(int) {
  auto old = *this;
  --*this;
  return old;
}

template <typename T>
SealedDSConstIterator<T>::Val SealedDSConstIterator<T>::operator*() {
  return shards_vec_iter_->run(
      +[](T::Shard &_, ContainerIter iter) { return *iter; }, container_iter_);
}

template <typename T>
template <class Archive>
void SealedDSConstIterator<T>::save(Archive &ar) const {
  uint64_t shards_vec_offset = shards_vec_iter_ - shards_->begin();
  ar(shards_, shards_vec_offset, container_iter_);
}

template <typename T>
template <class Archive>
void SealedDSConstIterator<T>::load(Archive &ar) {
  uint64_t shards_vec_offset;
  ar(shards_, shards_vec_offset, container_iter_);
  shards_vec_iter_ = shards_->begin() + shards_vec_offset;
}

template <typename T>
SealedDS<T>::SealedDS(T &&t) : t_(std::move(t)) {
  t_.seal();
  shards_ = std::make_shared<ShardsVec>(t_.get_all_non_empty_shards());
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
  return shards_->empty()
             ? ConstIterator()
             : ConstIterator(shards_, shards_->begin(),
                             shards_->front().run(&T::Shard::cbegin));
}

template <typename T>
SealedDS<T>::ConstIterator SealedDS<T>::cend() {
  return shards_->empty() ? ConstIterator()
                         : ConstIterator(shards_, --shards_->end(),
                                         shards_->back().run(&T::Shard::cend));
}

template <typename T>
SealedDS<T>::ConstReverseIterator SealedDS<T>::crbegin() {
  // TODO
  return ConstReverseIterator();
}

template <typename T>
SealedDS<T>::ConstReverseIterator SealedDS<T>::crend() {
  // TODO
  return ConstReverseIterator();
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
