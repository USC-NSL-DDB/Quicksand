namespace nu {

template <typename T>
inline VectorConstIterator<T>::VectorConstIterator() {}

template <typename T>
inline VectorConstIterator<T>::VectorConstIterator(
    std::vector<T>::iterator &&iter) {
  std::vector<T>::const_iterator::operator=(std::move(iter));
}

template <typename T>
inline VectorConstIterator<T>::VectorConstIterator(
    std::vector<T>::const_iterator &&iter) {
  std::vector<T>::const_iterator::operator=(std::move(iter));
}

template <typename T>
inline VectorConstReverseIterator<T>::VectorConstReverseIterator() {}

template <typename T>
inline VectorConstReverseIterator<T>::VectorConstReverseIterator(
    std::vector<T>::reverse_iterator &&iter) {
  std::vector<T>::const_reverse_iterator::operator=(std::move(iter));
}

template <typename T>
inline VectorConstReverseIterator<T>::VectorConstReverseIterator(
    std::vector<T>::const_reverse_iterator &&iter) {
  std::vector<T>::const_reverse_iterator::operator=(std::move(iter));
}

template <typename T>
inline Vector<T>::Vector() : l_key_(0), has_split_(false) {}

template <typename T>
inline Vector<T>::Vector(const Vector &o) {
  *this = o;
}

template <typename T>
inline Vector<T> &Vector<T>::operator=(const Vector &o) {
  data_ = o.data_;
  l_key_ = o.l_key_;
  return *this;
}

template <typename T>
inline Vector<T>::Vector(Vector &&o) noexcept {
  *this = std::move(o);
}

template <typename T>
inline Vector<T> &Vector<T>::operator=(Vector &&o) noexcept {
  data_ = std::move(o.data_);
  l_key_ = o.l_key_;
  return *this;
}

template <typename T>
inline std::size_t Vector<T>::size() const {
  return data_.size();
}

template <typename T>
inline std::size_t Vector<T>::capacity() const {
  return data_.capacity();
}

template <typename T>
inline void Vector<T>::reserve(std::size_t size) {
  return data_.reserve(size);
}

template <typename T>
inline bool Vector<T>::empty() const {
  return data_.empty();
}

template <typename T>
inline void Vector<T>::clear() {
  data_.clear();
}

template <typename T>
inline void Vector<T>::emplace(Key k, Val v) {
  data_[k - l_key_] = std::move(v);
}

template <typename T>
inline void Vector<T>::emplace_back(Val v) {
  data_.emplace_back(std::move(v));
}

template <typename T>
inline void Vector<T>::emplace_back_batch(std::vector<Val> v) {
  data_.insert(data_.end(), make_move_iterator(v.begin()),
               make_move_iterator(v.end()));
}

template <typename T>
inline void Vector<T>::merge(Vector vector) {
  data_.insert(data_.end(), std::make_move_iterator(vector.data_.begin()),
               std::make_move_iterator(vector.data_.end()));
}

template <typename T>
inline Vector<T>::ConstIterator Vector<T>::find(Key k) const {
  auto l_key = l_key_;
  auto r_key = l_key_ + data_.size();

  if (k < l_key || k >= r_key) {
    return data_.cend();
  }

  return data_.cbegin() + (k - l_key);
}

template <typename T>
void Vector<T>::split(Key *mid_k, Vector *latter_half) {
  auto new_shard_size = has_split_ ? data_.size() / 2 : 0;
  auto old_shard_size = data_.size() - new_shard_size;
  has_split_ = true;

  latter_half->data_.insert(
      latter_half->data_.end(),
      std::make_move_iterator(data_.end() - new_shard_size),
      std::make_move_iterator(data_.end()));

  latter_half->l_key_ = l_key_ + old_shard_size;
  *mid_k = latter_half->l_key_;
  data_.resize(old_shard_size);
}

template <typename T>
template <typename... S0s, typename... S1s>
inline void Vector<T>::for_all(void (*fn)(const Key &key, Val &val, S0s...),
                               S1s &&... states) {
  for (std::size_t i = 0; i < data_.size(); i++) {
    auto idx = l_key_ + i;
    fn(idx, data_[i], states...);
  }
}

template <typename T>
inline Vector<T>::ConstIterator Vector<T>::cbegin() const {
  return data_.cbegin();
}

template <typename T>
inline Vector<T>::ConstIterator Vector<T>::cend() const {
  return data_.cend();
}

template <typename T>
inline Vector<T>::ConstReverseIterator Vector<T>::crbegin() const {
  return data_.crbegin();
}

template <typename T>
inline Vector<T>::ConstReverseIterator Vector<T>::crend() const {
  return data_.crend();
}

template <typename T>
template <class Archive>
inline void Vector<T>::save(Archive &ar) const {
  ar(data_, l_key_);
}

template <typename T>
template <class Archive>
inline void Vector<T>::load(Archive &ar) {
  ar(data_, l_key_);
  has_split_ = false;
}

template <typename T, typename LL>
VectorInsertCollection<T, LL>::VectorInsertCollection(
    const ShardedVector<T, LL> &original)
    : original_(original), ref_cnt_(0) {}

template <typename T, typename LL>
VectorInsertCollection<T, LL>::~VectorInsertCollection() {
  flush();
}

template <typename T, typename LL>
void VectorInsertCollection<T, LL>::inc_ref_cnt() {
  mutex_.lock();
  ref_cnt_++;
  mutex_.unlock();
}

template <typename T, typename LL>
void VectorInsertCollection<T, LL>::dec_ref_cnt() {
  mutex_.lock();
  ref_cnt_--;
  mutex_.unlock();
}

template <typename T, typename LL>
void VectorInsertCollection<T, LL>::submit_batch(std::size_t rank,
                                                 ShardedVector<T, LL> elems) {
  mutex_.lock();
  vecs_.emplace(rank, std::move(elems));
  mutex_.unlock();
}
template <typename T, typename LL>
void VectorInsertCollection<T, LL>::flush() {
  mutex_.lock();
  assert(ref_cnt_ == 0);
  // TODO: optimize
  for (auto &[_, new_elems] : vecs_) {
    auto sealed_elems = nu::to_sealed_ds(std::move(new_elems));
    for (auto elem : sealed_elems) {
      original_.emplace_back(std::move(elem));
    }
  }
  mutex_.unlock();
}

template <typename T, typename LL>
VectorBackInserter<T, LL>::VectorBackInserter() {}

template <typename T, typename LL>
VectorBackInserter<T, LL>::VectorBackInserter(
    Proclet<VectorInsertCollection<T, LL>> state, std::size_t rank)
    : state_(state), elems_(make_sharded_vector<T, LL>()), rank_(rank) {
  state_.run(&VectorInsertCollection<T, LL>::inc_ref_cnt);
}

template <typename T, typename LL>
VectorBackInserter<T, LL>::VectorBackInserter(
    const VectorBackInserter<T, LL> &o)
    : state_(o.state_), elems_(o.elems_), rank_(o.rank_) {
  state_.run(&VectorInsertCollection<T, LL>::inc_ref_cnt);
}

template <typename T, typename LL>
VectorBackInserter<T, LL> &VectorBackInserter<T, LL>::operator=(
    const VectorBackInserter<T, LL> &o) {
  flush();
  state_ = o.state_;
  elems_ = o.elems_;
  rank_ = o.rank_;
  state_.run(&VectorInsertCollection<T, LL>::inc_ref_cnt);
  return *this;
}

template <typename T, typename LL>
VectorBackInserter<T, LL>::VectorBackInserter(
    VectorBackInserter<T, LL> &&o) noexcept
    : state_(std::move(o.state_)),
      elems_(std::move(o.elems_)),
      rank_(std::move(o.rank_)) {}

template <typename T, typename LL>
VectorBackInserter<T, LL> &VectorBackInserter<T, LL>::operator=(
    VectorBackInserter<T, LL> &&o) noexcept {
  flush();
  state_ = std::move(o.state_);
  elems_ = std::move(o.elems_);
  rank_ = std::move(o.rank_);
  return *this;
}

template <typename T, typename LL>
VectorBackInserter<T, LL>::~VectorBackInserter() {
  flush();
}

template <typename T, typename LL>
void VectorBackInserter<T, LL>::flush() {
  if (!state_) {
    return;
  }

  if (likely(elems_)) {
    elems_.value().flush();
    state_.run(
        +[](VectorInsertCollection<T, LL> &s, std::size_t rank,
            ShardedVector<T, LL> elems) {
          s.submit_batch(rank, std::move(elems));
          s.dec_ref_cnt();
        },
        rank_, elems_.value());
  } else {
    state_.run(&VectorInsertCollection<T, LL>::dec_ref_cnt);
  }
}

template <typename T, typename LL>
inline void VectorBackInserter<T, LL>::push_back(const T &elem) {
  if (likely(elems_)) {
    elems_.value().push_back(elem);
  } else {
    elems_ = make_sharded_vector<T, LL>();
    elems_.value().push_back(elem);
  }
}

template <typename T, typename LL>
inline void VectorBackInserter<T, LL>::emplace_back(T &&elem) {
  if (likely(elems_)) {
    elems_.value().emplace_back(elem);
  } else {
    elems_ = make_sharded_vector<T, LL>();
    elems_.value().emplace_back(elem);
  }
}

template <typename T, typename LL>
VectorBackInserter<T, LL> VectorBackInserter<T, LL>::split(
    std::size_t next_inserter_rank) {
  assert(next_inserter_rank > rank_);
  return VectorBackInserter(state_, next_inserter_rank);
}

template <typename T, typename LL>
template <class Archive>
void VectorBackInserter<T, LL>::save(Archive &ar) const {
  ar(state_, elems_, rank_);
}

template <typename T, typename LL>
template <class Archive>
void VectorBackInserter<T, LL>::load(Archive &ar) {
  ar(state_, elems_, rank_);
  state_.run(&VectorInsertCollection<T, LL>::inc_ref_cnt);
}

template <typename T, typename LL>
inline ShardedVector<T, LL>::ShardedVector() {}

template <typename T, typename LL>
inline T ShardedVector<T, LL>::operator[](std::size_t index) const {
  std::optional<T> r = this->find_data(index);
  return *r;
}

template <typename T, typename LL>
inline void ShardedVector<T, LL>::set(std::size_t index, T value) {
  Base::emplace(index, std::move(value));
}

template <typename T, typename LL>
inline void ShardedVector<T, LL>::push_back(const T &value) {
  Base::emplace_back(value);
}

template <typename T, typename LL>
inline void ShardedVector<T, LL>::emplace_back(T &&value) {
  Base::emplace_back(std::move(value));
}

template <typename T, typename LL>
inline VectorBackInserter<T, LL> ShardedVector<T, LL>::back_inserter() {
  auto size = this->size();
  auto state = make_proclet<VectorInsertCollection<T, LL>>(*this);
  return VectorBackInserter(state, size);
}

template <typename T, typename LL>
inline ShardedVector<T, LL>::ShardedVector(
    std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename T, typename LL>
inline ShardedVector<T, LL> make_sharded_vector() {
  return ShardedVector<T, LL>(std::nullopt);
}

template <typename T, typename LL>
inline ShardedVector<T, LL> make_sharded_vector(uint64_t reserved_count) {
  return ShardedVector<T, LL>(typename ShardedVector<T, LL>::Base::Hint(
      reserved_count, 0, [](Vector<T>::Key &k, uint64_t off) { k += off; }));
}

}  // namespace nu
