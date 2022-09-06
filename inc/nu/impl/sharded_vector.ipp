#include <cereal/types/vector.hpp>

namespace nu {

template <typename T>
VectorConstIterator<T>::VectorConstIterator() {}

template <typename T>
VectorConstIterator<T>::VectorConstIterator(std::vector<T>::iterator &&iter) {
  std::vector<T>::const_iterator::operator=(std::move(iter));
}

template <typename T>
VectorConstIterator<T>::VectorConstIterator(
    std::vector<T>::const_iterator &&iter) {
  std::vector<T>::const_iterator::operator=(std::move(iter));
}

template <typename T>
template <class Archive>
void VectorConstIterator<T>::serialize(Archive &ar) {
  ar(cereal::binary_data(this, sizeof(*this)));
}

template <typename T>
VectorConstReverseIterator<T>::VectorConstReverseIterator() {}

template <typename T>
VectorConstReverseIterator<T>::VectorConstReverseIterator(
    std::vector<T>::reverse_iterator &&iter) {
  std::vector<T>::const_reverse_iterator::operator=(std::move(iter));
}

template <typename T>
VectorConstReverseIterator<T>::VectorConstReverseIterator(
    std::vector<T>::const_reverse_iterator &&iter) {
  std::vector<T>::const_reverse_iterator::operator=(std::move(iter));
}

template <typename T>
template <class Archive>
void VectorConstReverseIterator<T>::serialize(Archive &ar) {
  ar(cereal::binary_data(this, sizeof(*this)));
}

template <typename T>
Vector<T>::Vector() : l_key_(0) {}

template <typename T>
Vector<T>::Vector(std::size_t capacity) : l_key_(0) {
  data_.reserve(capacity);
}

template <typename T>
Vector<T>::Vector(const Vector &o) {
  *this = o;
}

template <typename T>
Vector<T> &Vector<T>::operator=(const Vector &o) {
  data_ = o.data_;
  l_key_ = o.l_key_;
  return *this;
}

template <typename T>
Vector<T>::Vector(Vector &&o) noexcept {
  *this = std::move(o);
}

template <typename T>
Vector<T> &Vector<T>::operator=(Vector &&o) noexcept {
  data_ = std::move(o.data_);
  l_key_ = o.l_key_;
  return *this;
}

template <typename T>
std::size_t Vector<T>::size() const {
  return data_.size();
}

template <typename T>
std::size_t Vector<T>::capacity() const {
  return data_.capacity_;
}

template <typename T>
bool Vector<T>::empty() const {
  return data_.empty();
}

template <typename T>
void Vector<T>::clear() {
  data_.clear();
}

template <typename T>
void Vector<T>::emplace(Key k, Val v) {
  BUG();
}

template <typename T>
void Vector<T>::emplace_back(Val v) {
  data_.emplace_back(std::move(v));
}

template <typename T>
void Vector<T>::merge(Vector vector) {
  data_.insert(data_.end(), std::make_move_iterator(vector.data_.begin()),
               std::make_move_iterator(vector.data_.end()));
}

template <typename T>
Vector<T>::ConstIterator Vector<T>::find(Key k) {
  auto l_key = l_key_;
  auto r_key = l_key_ + data_.size();

  if (k < l_key || k >= r_key) {
    return data_.cend();
  }

  return data_.cbegin() + (k - l_key);
}

template <typename T>
std::pair<typename Vector<T>::Key, Vector<T>> Vector<T>::split() {
  auto new_shard_l_key = l_key_ + data_.size();
  Vector<T> new_vec;
  new_vec.l_key_ = new_shard_l_key;
  return std::make_pair(new_shard_l_key, std::move(new_vec));
}

template <typename T>
template <typename... S0s, typename... S1s>
void Vector<T>::for_all(void (*fn)(const Key &key, Val &val, S0s...),
                        S1s &&... states) {
  for (std::size_t i = 0; i < data_.size(); i++) {
    auto idx = l_key_ + i;
    fn(idx, data_[i], states...);
  }
}

template <typename T>
Vector<T>::ConstIterator Vector<T>::cbegin() const {
  return data_.cbegin();
}

template <typename T>
Vector<T>::ConstIterator Vector<T>::cend() const {
  return data_.cend();
}

template <typename T>
Vector<T>::ConstReverseIterator Vector<T>::crbegin() const {
  return data_.crbegin();
}

template <typename T>
Vector<T>::ConstReverseIterator Vector<T>::crend() const {
  return data_.crend();
}

template <typename T>
template <class Archive>
void Vector<T>::save(Archive &ar) const {
  ar(data_, l_key_);
}

template <typename T>
template <class Archive>
void Vector<T>::load(Archive &ar) {
  ar(data_, l_key_);
}

template <typename T, typename LL>
ShardedVector<T, LL>::ShardedVector() {}

template <typename T, typename LL>
T ShardedVector<T, LL>::operator[](std::size_t index) const {
  std::optional<T> r = this->find_val(index);
  return *r;
}

template <typename T, typename LL>
void ShardedVector<T, LL>::push_back(const T &value) {
  auto copy = value;
  Base::emplace_back(std::move(copy));
}

template <typename T, typename LL>
void ShardedVector<T, LL>::emplace_back(T &&value) {
  Base::emplace_back(std::move(value));
}

template <typename T, typename LL>
ShardedVector<T, LL>::ShardedVector(std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename T, typename LL>
ShardedVector<T, LL> make_sharded_vector() {
  return ShardedVector<T, LL>(std::nullopt);
}

template <typename T, typename LL>
ShardedVector<T, LL> make_sharded_vector(uint64_t reserved_count) {
  return ShardedVector<T, LL>(typename ShardedVector<T, LL>::Base::Hint(
      reserved_count, 0, [](Vector<T>::Key &k, uint64_t off) { k += off; }));
}

}  // namespace nu
