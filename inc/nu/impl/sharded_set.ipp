#include <iterator>
#include <optional>
#include <utility>

namespace nu {

template <class Set>
SetConstIterator<Set>::SetConstIterator() {}

template <class Set>
SetConstIterator<Set>::SetConstIterator(Set::iterator &&iter) {
  Set::const_iterator::operator=(std::move(iter));
}

template <class Set>
SetConstReverseIterator<Set>::SetConstReverseIterator() {}

template <class Set>
SetConstReverseIterator<Set>::SetConstReverseIterator(
    Set::reverse_iterator &&iter) {
  Set::const_reverse_iterator::operator=(std::move(iter));
}

template <typename T, typename M>
GeneralSet<T, M>::GeneralSet() {}

template <typename T, typename M>
GeneralSet<T, M>::GeneralSet(std::optional<Key> l_key) {}

template <typename T, typename M>
GeneralSet<T, M>::GeneralSet(std::optional<Key> l_key, std::size_t capacity) {}

template <typename T, typename M>
std::size_t GeneralSet<T, M>::size() const {
  return set_.size();
}

template <typename T, typename M>
bool GeneralSet<T, M>::empty() const {
  return set_.empty();
}

template <typename T, typename M>
void GeneralSet<T, M>::clear() {
  set_.clear();
}

template <typename T, typename M>
void GeneralSet<T, M>::emplace(Key k, Val v) {
  assert(k == v);
  set_.insert(std::move(k));
}

template <typename T, typename M>
GeneralSet<T, M>::ConstIterator GeneralSet<T, M>::find(Key k) {
  return set_.find(std::move(k));
}

template <typename T, typename M>
void GeneralSet<T, M>::merge(GeneralSet s) {
  set_.insert(std::make_move_iterator(s.set_.begin()),
              std::make_move_iterator(s.set_.end()));
}

template <typename T, typename M>
template <typename... S0s, typename... S1s>
void GeneralSet<T, M>::for_all(void (*fn)(const Key &key, Val &val, S0s...),
                               S1s &&... states) {
  assert(false /* not implemented */);
}

template <typename T, typename M>
GeneralSet<T, M>::GeneralSet(Set initial_state) {
  set_ = std::move(initial_state);
}

template <typename T, typename M>
std::pair<typename GeneralSet<T, M>::Key, GeneralSet<T, M>>
GeneralSet<T, M>::split() {
  assert(set_.size() > 0);
  auto mid = set_.size() / 2;

  auto latter_half_begin_itr = set_.begin();
  std::advance(latter_half_begin_itr, mid);
  auto latter_half_l_key = *latter_half_begin_itr;

  Set latter_half_set(latter_half_begin_itr, set_.end());
  set_.erase(latter_half_begin_itr, set_.end());

  GeneralSet latter_half_container(std::move(latter_half_set));

  return std::make_pair(latter_half_l_key, std::move(latter_half_container));
}

template <typename T, typename M>
GeneralSet<T, M>::Set &GeneralSet<T, M>::data() {
  return set_;
}

template <typename T, typename M>
GeneralSet<T, M>::ConstIterator GeneralSet<T, M>::cbegin() const {
  return set_.cbegin();
}

template <typename T, typename M>
GeneralSet<T, M>::ConstIterator GeneralSet<T, M>::cend() const {
  return set_.cend();
}

template <typename T, typename M>
GeneralSet<T, M>::ConstReverseIterator GeneralSet<T, M>::crbegin() const {
  return set_.crbegin();
}

template <typename T, typename M>
GeneralSet<T, M>::ConstReverseIterator GeneralSet<T, M>::crend() const {
  return set_.crend();
}

template <typename T, typename M>
template <class Archive>
void GeneralSet<T, M>::save(Archive &ar) const {
  ar(set_);
}

template <typename T, typename M>
template <class Archive>
void GeneralSet<T, M>::load(Archive &ar) {
  ar(set_);
}

template <typename T, typename M, typename LL>
void GeneralShardedSet<T, M, LL>::insert(const T &value) {
  this->emplace(value, value);
}

template <typename T, typename M, typename LL>
bool GeneralShardedSet<T, M, LL>::empty() {
  return this->size() == 0;
}

template <typename T, typename M, typename LL>
GeneralShardedSet<T, M, LL>::GeneralShardedSet(
    std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename T, typename LL>
ShardedSet<T, LL> make_sharded_set() {
  return ShardedSet<T, LL>(std::nullopt);
}

template <typename T, typename LL>
ShardedMultiSet<T, LL> make_sharded_multi_set() {
  return ShardedMultiSet<T, LL>(std::nullopt);
}

}  // namespace nu
