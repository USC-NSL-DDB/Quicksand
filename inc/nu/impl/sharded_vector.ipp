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
