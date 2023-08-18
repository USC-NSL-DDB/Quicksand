#include <iterator>
#include <optional>
#include <utility>

namespace nu {

template <class Set>
inline SetConstIterator<Set>::SetConstIterator() {}

template <class Set>
inline SetConstIterator<Set>::SetConstIterator(Set::iterator &&iter) {
  Set::const_iterator::operator=(std::move(iter));
}

template <class Set>
inline SetConstReverseIterator<Set>::SetConstReverseIterator() {}

template <class Set>
inline SetConstReverseIterator<Set>::SetConstReverseIterator(
    Set::reverse_iterator &&iter) {
  Set::const_reverse_iterator::operator=(std::move(iter));
}

template <typename T, typename M>
inline GeneralSet<T, M>::GeneralSet() {}

template <typename T, typename M>
inline std::size_t GeneralSet<T, M>::size() const {
  return set_.size();
}

template <typename T, typename M>
inline bool GeneralSet<T, M>::empty() const {
  return set_.empty();
}

template <typename T, typename M>
inline void GeneralSet<T, M>::clear() {
  set_.clear();
}

template <typename T, typename M>
inline std::size_t GeneralSet<T, M>::insert(Key k) {
  set_.insert(std::move(k));
  return set_.size();
}

template <typename T, typename M>
inline GeneralSet<T, M>::ConstIterator GeneralSet<T, M>::find(Key k) const {
  return set_.find(std::move(k));
}

template <typename T, typename M>
inline void GeneralSet<T, M>::merge(GeneralSet s) {
  set_.merge(std::move(s.set_));
}

template <typename T, typename M>
template <typename... S0s, typename... S1s>
inline void GeneralSet<T, M>::for_all(void (*fn)(const Key &key, S0s...),
                                      S1s &&... states) {
  // Not implemented.
  BUG();
}

template <typename T, typename M>
inline GeneralSet<T, M>::GeneralSet(Set initial_state) {
  set_ = std::move(initial_state);
}

template <typename T, typename M>
void GeneralSet<T, M>::split(Key *mid_k, GeneralSet *latter_half) {
  auto mid = set_.size() / 2;

  auto latter_half_begin_itr = set_.begin();
  std::advance(latter_half_begin_itr, mid);
  *mid_k = *latter_half_begin_itr;

  latter_half->set_.insert(std::make_move_iterator(latter_half_begin_itr),
                           std::make_move_iterator(set_.end()));
  set_.erase(latter_half_begin_itr, set_.end());
}

template <typename T, typename M>
inline GeneralSet<T, M>::Set &GeneralSet<T, M>::data() {
  return set_;
}

template <typename T, typename M>
inline GeneralSet<T, M>::ConstIterator GeneralSet<T, M>::cbegin() const {
  return set_.cbegin();
}

template <typename T, typename M>
inline GeneralSet<T, M>::ConstIterator GeneralSet<T, M>::cend() const {
  return set_.cend();
}

template <typename T, typename M>
inline GeneralSet<T, M>::ConstReverseIterator GeneralSet<T, M>::crbegin()
    const {
  return set_.crbegin();
}

template <typename T, typename M>
inline GeneralSet<T, M>::ConstReverseIterator GeneralSet<T, M>::crend() const {
  return set_.crend();
}

template <typename T, typename M>
template <class Archive>
inline void GeneralSet<T, M>::save(Archive &ar) const {
  ar(set_);
}

template <typename T, typename M>
template <class Archive>
inline void GeneralSet<T, M>::load(Archive &ar) {
  ar(set_);
}

template <typename T, typename M, typename LL>
inline GeneralShardedSet<T, M, LL>::GeneralShardedSet(
    std::optional<typename Base::ShardingHint> sharding_hint)
    : Base(sharding_hint, /* size_bound = */ std::nullopt,
           /* pinned_ip = */ std::nullopt) {}

template <typename T, typename LL>
inline ShardedSet<T, LL> make_sharded_set() {
  return ShardedSet<T, LL>(std::nullopt);
}

template <typename T, typename LL>
inline ShardedMultiSet<T, LL> make_sharded_multi_set() {
  return ShardedMultiSet<T, LL>(std::nullopt);
}

}  // namespace nu
