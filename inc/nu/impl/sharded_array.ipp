#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>

#include "nu/commons.hpp"

namespace nu {
template <typename T>
ArrayConstIterator<T>::ArrayConstIterator() {}

template <typename T>
ArrayConstIterator<T>::ArrayConstIterator(std::vector<T>::iterator &&iter) {
  std::vector<T>::const_iterator::operator=(std::move(iter));
}

template <typename T>
ArrayConstIterator<T>::ArrayConstIterator(
    std::vector<T>::const_iterator &&iter) {
  std::vector<T>::const_iterator::operator=(std::move(iter));
}

template <typename T>
template <class Archive>
void ArrayConstIterator<T>::serialize(Archive &ar) {
  ar(cereal::binary_data(this, sizeof(*this)));
}

template <typename T>
ArrayConstReverseIterator<T>::ArrayConstReverseIterator() {}

template <typename T>
ArrayConstReverseIterator<T>::ArrayConstReverseIterator(
    std::vector<T>::reverse_iterator &&iter) {
  std::vector<T>::const_reverse_iterator::operator=(std::move(iter));
}

template <typename T>
ArrayConstReverseIterator<T>::ArrayConstReverseIterator(
    std::vector<T>::const_reverse_iterator &&iter) {
  std::vector<T>::const_reverse_iterator::operator=(std::move(iter));
}

template <typename T>
template <class Archive>
void ArrayConstReverseIterator<T>::serialize(Archive &ar) {
  ar(cereal::binary_data(this, sizeof(*this)));
}

template <typename T>
Array<T>::Array() {}

template <typename T>
Array<T>::Array(std::optional<Key> l_key) : l_key_(l_key.value_or(0)) {}

template <typename T>
Array<T>::Array(std::optional<Key> l_key, std::size_t size)
    : l_key_(l_key.value_or(0)), data_(size, T()) {}

template <typename T>
Array<T>::Array(const Array &o) {
  *this = o;
}

template <typename T>
Array<T> &Array<T>::operator=(const Array &o) {
  data_ = o.data_;
  l_key_ = o.l_key_;
  return *this;
}

template <typename T>
Array<T>::Array(Array &&o) noexcept {
  *this = std::move(o);
}

template <typename T>
Array<T> &Array<T>::operator=(Array &&o) noexcept {
  data_ = std::move(o.data_);
  l_key_ = o.l_key_;
  return *this;
}

template <typename T>
std::size_t Array<T>::size() const {
  return data_.size();
}

template <typename T>
bool Array<T>::empty() const {
  return data_.empty();
}

template <typename T>
void Array<T>::emplace(Key k, Val v) {
  auto idx = k - l_key_;
  assert(idx < data_.size());
  data_[idx] = v;
}

template <typename T>
void Array<T>::merge(Array arr) {
  data_.insert(data_.end(), std::make_move_iterator(arr.data_.begin()),
               std::make_move_iterator(arr.data_.end()));
}

template <typename T>
std::pair<typename Array<T>::Key, Array<T>> Array<T>::split() {
  BUG();
}

template <typename T>
Array<T>::ConstIterator Array<T>::find(Key k) {
  auto l_key = l_key_;
  auto r_key = l_key_ + data_.size();

  if (k < l_key || k >= r_key) {
    return data_.cend();
  }

  return data_.cbegin() + (k - l_key);
}

template <typename T>
Array<T>::ConstIterator Array<T>::cbegin() const {
  return data_.cbegin();
}

template <typename T>
Array<T>::ConstIterator Array<T>::cend() const {
  return data_.cend();
}

template <typename T>
Array<T>::ConstReverseIterator Array<T>::crbegin() const {
  return data_.crbegin();
}

template <typename T>
Array<T>::ConstReverseIterator Array<T>::crend() const {
  return data_.crend();
}

template <typename T>
template <class Archive>
void Array<T>::save(Archive &ar) const {
  ar(data_, l_key_);
}

template <typename T>
template <class Archive>
void Array<T>::load(Archive &ar) {
  ar(data_, l_key_);
}

template <typename T, typename LL>
ShardedArray<T, LL>::ShardedArray() {}

template <typename T, typename LL>
T ShardedArray<T, LL>::operator[](std::size_t index) const {
  std::optional<T> r = this->find_val(index);
  return *r;
}

template <typename T, typename LL>
void ShardedArray<T, LL>::set(std::size_t index, T value) {
  Base::emplace(index, std::move(value));
}

template <typename T, typename LL>
ShardedArray<T, LL>::ShardedArray(std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename T, typename LL>
ShardedArray<T, LL> make_sharded_array(std::size_t size) {
  return ShardedArray<T, LL>(typename ShardedArray<T, LL>::Base::Hint(
      size, 0, [](Array<T>::Key &k, uint64_t shard_size) { k += shard_size; }));
}

}  // namespace nu
