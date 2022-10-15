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

template <typename T, typename M>
std::size_t GeneralUnorderedSet<T, M>::size() const {
  return set_.size();
}

template <typename T, typename M>
void GeneralUnorderedSet<T, M>::reserve(std::size_t size) {
  return set_.reserve(size);
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
    Key k) const {
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
void GeneralUnorderedSet<T, M>::split(Key *mid_k,
                                      GeneralUnorderedSet *latter_half) {
  std::vector<T> keys;
  keys.reserve(set_.size());
  for (const auto &k : set_) {
    keys.push_back(k);
  }

  std::nth_element(keys.begin(), keys.begin() + keys.size() / 2, keys.end());
  *mid_k = keys[keys.size() / 2];

  for (auto it = set_.cbegin(); it != set_.cend();) {
    if (*it >= *mid_k) {
      latter_half->set_.emplace(std::move(*it));
      it = set_.erase(it);
    } else {
      ++it;
    }
  }
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
