#include <cereal/types/vector.hpp>

namespace nu {
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
  if (unlikely(data_.empty())) {
    l_key_ = k;
  }
  data_.emplace_back(std::move(v));
}

template <typename T>
void Vector<T>::emplace_batch(Vector &&vector) {
  data_.insert(data_.end(), std::make_move_iterator(vector.data_.begin()),
               std::make_move_iterator(vector.data_.end()));
}

template <typename T>
std::optional<T> Vector<T>::find_val(Key k) {
  auto l_key = l_key_;
  auto r_key = l_key_ + data_.size();

  if (k < l_key || k >= r_key) {
    return std::nullopt;
  }

  return data_[k - l_key];
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
template <class Archive>
void Vector<T>::save(Archive &ar) const {
  ar(data_, l_key_);
}

template <typename T>
template <class Archive>
void Vector<T>::load(Archive &ar) {
  ar(data_, l_key_);
}

template <typename T, bool LowLat>
ShardedVector<T, LowLat>::ShardedVector() : size_(0) {}

template <typename T, bool LowLat>
ShardedVector<T, LowLat>::ShardedVector(const ShardedVector &o) {
  *this = o;
}

template <typename T, bool LowLat>
ShardedVector<T, LowLat> &ShardedVector<T, LowLat>::operator=(
    const ShardedVector &o) {
  size_ = o.size_;
  return *this;
}

template <typename T, bool LowLat>
ShardedVector<T, LowLat>::ShardedVector(ShardedVector &&o) noexcept {
  *this = std::move(o);
}

template <typename T, bool LowLat>
ShardedVector<T, LowLat> &ShardedVector<T, LowLat>::operator=(
    ShardedVector &&o) noexcept {
  size_ = o.size_;
  return *this;
}

template <typename T, bool LowLat>
T ShardedVector<T, LowLat>::operator[](std::size_t index) {
  assert(index < size_);
  std::optional<T> r = this->find_val(index);
  return *r;
}

template <typename T, bool LowLat>
void ShardedVector<T, LowLat>::push_back(const T &value) {
  this->emplace(size_, value);
  size_++;
}

template <typename T, bool LowLat>
void ShardedVector<T, LowLat>::pop_back() {
  size_--;
}

template <typename T, bool LowLat>
std::size_t ShardedVector<T, LowLat>::size() {
  return size_;
}

template <typename T, bool LowLat>
bool ShardedVector<T, LowLat>::empty() {
  return !size_;
}

template <typename T, bool LowLat>
void ShardedVector<T, LowLat>::clear() {
  size_ = 0;
}

template <typename T, bool LowLat>
ShardedVector<T, LowLat>::ShardedVector(std::optional<typename Base::Hint> hint)
    : Base(hint), size_(0) {}

template <typename T, bool LowLat>
ShardedVector<T, LowLat> make_sharded_vector() {
  return ShardedVector<T, LowLat>(std::nullopt);
}

}  // namespace nu
