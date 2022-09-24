#include <cereal/types/unordered_set.hpp>
#include <iterator>
#include <optional>
#include <utility>

namespace nu {

template <class USet>
UnorderedSetConstIterator<USet>::UnorderedSetConstIterator() {}

template <class USet>
UnorderedSetConstIterator<USet>::UnorderedSetConstIterator(
    USet::iterator &&iter) {
  USet::const_iterator::operator=(std::move(iter));
}

template <class USet>
UnorderedSetConstIterator<USet>::UnorderedSetConstIterator(
    USet::const_iterator &&iter) {
  USet::const_iterator::operator=(std::move(iter));
}

template <class USet>
template <class Archive>
void UnorderedSetConstIterator<USet>::serialize(Archive &ar) {
  ar(cereal::binary_data(this, sizeof(*this)));
}

template <typename T, typename M>
GeneralUnorderedSet<T, M>::GeneralUnorderedSet(std::optional<Key> l_key) {}

template <typename T, typename M>
GeneralUnorderedSet<T, M>::GeneralUnorderedSet(std::optional<Key> l_key,
                                               std::size_t capacity) {}

template <typename T, typename M>
std::size_t GeneralUnorderedSet<T, M>::size() const {
  return set_.size();
}

template <typename T, typename M>
bool GeneralUnorderedSet<T, M>::empty() const {
  return set_.empty();
}

template <typename T, typename M>
void GeneralUnorderedSet<T, M>::clear() {
  set_.clear();
}

template <typename T, typename M>
void GeneralUnorderedSet<T, M>::emplace(Key k, Val v) {
  assert(k == v);
  set_.insert(std::move(k));
}

template <typename T, typename M>
GeneralUnorderedSet<T, M>::ConstIterator GeneralUnorderedSet<T, M>::find(
    Key k) {
  return set_.find(std::move(k));
}

template <typename T, typename M>
void GeneralUnorderedSet<T, M>::merge(GeneralUnorderedSet s) {
  set_.insert(std::make_move_iterator(s.set_.begin()),
              std::make_move_iterator(s.set_.end()));
}

template <typename T, typename M>
template <typename... S0s, typename... S1s>
void GeneralUnorderedSet<T, M>::for_all(void (*fn)(const Key &key, Val &val,
                                                   S0s...),
                                        S1s &&... states) {
  /* Not implemented */
  BUG();
}

template <typename T, typename M>
GeneralUnorderedSet<T, M>::GeneralUnorderedSet(USet initial_state)
    : set_(std::move(initial_state)) {}

template <typename T, typename M>
std::pair<T, GeneralUnorderedSet<T, M>> GeneralUnorderedSet<T, M>::split() {
  assert(set_.size() > 0);
  auto mid = set_.size() / 2;

  auto latter_half_begin_itr = set_.begin();
  std::advance(latter_half_begin_itr, mid);
  auto latter_half_l_key = *latter_half_begin_itr;

  USet latter_half_set(latter_half_begin_itr, set_.end());
  set_.erase(latter_half_begin_itr, set_.end());

  GeneralUnorderedSet latter_half_container(std::move(latter_half_set));

  return std::make_pair(latter_half_l_key, std::move(latter_half_container));
}

template <typename T, typename M>
GeneralUnorderedSet<T, M>::USet &GeneralUnorderedSet<T, M>::data() {
  return set_;
}

template <typename T, typename M>
GeneralUnorderedSet<T, M>::ConstIterator GeneralUnorderedSet<T, M>::cbegin()
    const {
  return set_.cbegin();
}

template <typename T, typename M>
GeneralUnorderedSet<T, M>::ConstIterator GeneralUnorderedSet<T, M>::cend()
    const {
  return set_.cend();
}

template <typename T, typename M>
template <class Archive>
void GeneralUnorderedSet<T, M>::save(Archive &ar) const {
  ar(set_);
}

template <typename T, typename M>
template <class Archive>
void GeneralUnorderedSet<T, M>::load(Archive &ar) {
  ar(set_);
}

template <typename T, typename M, typename LL>
void GeneralShardedUnorderedSet<T, M, LL>::insert(const T &value) {
  this->emplace(value, value);
}

template <typename T, typename M, typename LL>
bool GeneralShardedUnorderedSet<T, M, LL>::empty() {
  return this->size() == 0;
}

template <typename T, typename M, typename LL>
GeneralShardedUnorderedSet<T, M, LL>::GeneralShardedUnorderedSet(
    std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename T, typename LL>
ShardedUnorderedSet<T, LL> make_sharded_unordered_set() {
  return ShardedUnorderedSet<T, LL>(std::nullopt);
}

template <typename T, typename LL>
ShardedUnorderedMultiSet<T, LL> make_sharded_unordered_multiset() {
  return ShardedUnorderedMultiSet<T, LL>(std::nullopt);
}

}  // namespace nu
