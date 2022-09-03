#include <cereal/types/unordered_set.hpp>
#include <iterator>
#include <optional>
#include <utility>

namespace nu {
template <typename T>
UnorderedSetConstIterator<T>::UnorderedSetConstIterator() {}

template <typename T>
UnorderedSetConstIterator<T>::UnorderedSetConstIterator(
    std::unordered_set<T>::const_iterator &&iter) {
  std::unordered_set<T>::const_iterator::operator=(std::move(iter));
}

template <typename T>
template <class Archive>
void UnorderedSetConstIterator<T>::serialize(Archive &ar) {
  ar(cereal::binary_data(this, sizeof(*this)));
}

template <typename T>
UnorderedSet<T>::UnorderedSet() {}

template <typename T>
UnorderedSet<T>::UnorderedSet(std::size_t capacity) {}

template <typename T>
std::size_t UnorderedSet<T>::size() const {
  return set_.size();
}

template <typename T>
bool UnorderedSet<T>::empty() const {
  return set_.empty();
}

template <typename T>
void UnorderedSet<T>::clear() {
  set_.clear();
}

template <typename T>
void UnorderedSet<T>::emplace(Key k, Val v) {
  assert(k == v);
  set_.insert(std::move(k));
}

template <typename T>
void UnorderedSet<T>::emplace_back(Val v) {
  BUG();
}

template <typename T>
void UnorderedSet<T>::merge(UnorderedSet s) {
  set_.insert(std::make_move_iterator(s.set_.begin()),
              std::make_move_iterator(s.set_.end()));
}

template <typename T>
template <typename... S0s, typename... S1s>
void UnorderedSet<T>::for_all(void (*fn)(const Key &key, Val &val, S0s...),
                              S1s &&... states) {
  assert(false /* not implemented */);
}

template <typename T>
std::pair<typename UnorderedSet<T>::Key, UnorderedSet<T>>
UnorderedSet<T>::split() {
  assert(set_.size() > 0);
  auto mid = set_.size() / 2;

  auto latter_half_begin_itr = set_.begin();
  std::advance(latter_half_begin_itr, mid);
  auto latter_half_l_key = *latter_half_begin_itr;

  std::unordered_set<T> latter_half_set(latter_half_begin_itr, set_.end());
  set_.erase(latter_half_begin_itr, set_.end());

  UnorderedSet latter_half_container;
  latter_half_container.set_ = std::move(latter_half_set);

  return std::make_pair(latter_half_l_key, std::move(latter_half_container));
}

template <typename T>
std::unordered_set<T> &UnorderedSet<T>::data() {
  return set_;
}

template <typename T>
UnorderedSet<T>::ConstIterator UnorderedSet<T>::cbegin() const {
  return set_.cbegin();
}

template <typename T>
UnorderedSet<T>::ConstIterator UnorderedSet<T>::cend() const {
  return set_.cend();
}

template <typename T>
UnorderedSet<T>::ConstReverseIterator UnorderedSet<T>::crbegin() const {
  return NoopIterator<T>();
}

template <typename T>
UnorderedSet<T>::ConstReverseIterator UnorderedSet<T>::crend() const {
  return NoopIterator<T>();
}

template <typename T>
template <class Archive>
void UnorderedSet<T>::save(Archive &ar) const {
  ar(set_);
}

template <typename T>
template <class Archive>
void UnorderedSet<T>::load(Archive &ar) {
  ar(set_);
}

template <typename T, typename LL>
void ShardedUnorderedSet<T, LL>::insert(const T &value) {
  this->emplace(value, value);
}

template <typename T, typename LL>
bool ShardedUnorderedSet<T, LL>::empty() {
  return this->size() == 0;
}

template <typename T, typename LL>
ShardedUnorderedSet<T, LL>::ShardedUnorderedSet(
    std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename T, typename LL>
ShardedUnorderedSet<T, LL> make_sharded_unordered_set() {
  return ShardedUnorderedSet<T, LL>(std::nullopt);
}

}  // namespace nu
