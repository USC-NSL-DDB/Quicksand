#include <cereal/types/set.hpp>
#include <iterator>
#include <optional>
#include <utility>

namespace nu {
template <typename T>
Set<T>::Set() {}

template <typename T>
Set<T>::Set(std::size_t capacity) {}

template <typename T>
std::size_t Set<T>::size() const {
  return set_.size();
}

template <typename T>
bool Set<T>::empty() const {
  return set_.empty();
}

template <typename T>
void Set<T>::clear() {
  set_.clear();
}

template <typename T>
void Set<T>::emplace(Key k, Val v) {
  assert(k == v);
  set_.insert(std::move(k));
}

template <typename T>
void Set<T>::emplace_batch(Set &&s) {
  set_.insert(std::make_move_iterator(s.set_.begin()),
              std::make_move_iterator(s.set_.end()));
}

template <typename T>
template <typename... S0s, typename... S1s>
void Set<T>::for_all(void (*fn)(const Key &key, Val &val, S0s...),
                     S1s &&... states) {
  assert(false /* not implemented */);
}

template <typename T>
std::pair<typename Set<T>::Key, Set<T>> Set<T>::split() {
  assert(set_.size() > 0);
  auto mid = set_.size() / 2;

  auto latter_half_begin_itr = set_.begin();
  std::advance(latter_half_begin_itr, mid);
  auto latter_half_l_key = *latter_half_begin_itr;

  std::set<T> latter_half_set(latter_half_begin_itr, set_.end());
  set_.erase(latter_half_begin_itr, set_.end());

  Set latter_half_container;
  latter_half_container.set_ = std::move(latter_half_set);

  return std::make_pair(latter_half_l_key, std::move(latter_half_container));
}

template <typename T>
std::set<T> &Set<T>::data() {
  return set_;
}

template <typename T>
template <class Archive>
void Set<T>::save(Archive &ar) const {
  ar(set_);
}

template <typename T>
template <class Archive>
void Set<T>::load(Archive &ar) {
  ar(set_);
}

template <typename T, typename LL>
void ShardedSet<T, LL>::insert(const T &value) {
  this->emplace(value, value);
}

template <typename T, typename LL>
bool ShardedSet<T, LL>::empty() {
  return this->size() == 0;
}

template <typename T, typename LL>
ShardedSet<T, LL>::ShardedSet(std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename T, typename LL>
ShardedSet<T, LL> make_sharded_set() {
  return ShardedSet<T, LL>(std::nullopt);
}

}  // namespace nu
