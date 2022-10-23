#include <iterator>
#include <optional>
#include <utility>

namespace nu {

template <class USet>
inline UnorderedSetConstIterator<USet>::UnorderedSetConstIterator() {}

template <class USet>
inline UnorderedSetConstIterator<USet>::UnorderedSetConstIterator(
    USet::iterator &&iter) {
  USet::const_iterator::operator=(std::move(iter));
}

template <class USet>
inline UnorderedSetConstIterator<USet>::UnorderedSetConstIterator(
    USet::const_iterator &&iter) {
  USet::const_iterator::operator=(std::move(iter));
}

template <typename T, typename M>
inline std::size_t GeneralUnorderedSet<T, M>::size() const {
  return set_.size();
}

template <typename T, typename M>
inline void GeneralUnorderedSet<T, M>::reserve(std::size_t size) {
  return set_.reserve(size);
}

template <typename T, typename M>
inline bool GeneralUnorderedSet<T, M>::empty() const {
  return set_.empty();
}

template <typename T, typename M>
inline void GeneralUnorderedSet<T, M>::clear() {
  set_.clear();
}

template <typename T, typename M>
inline void GeneralUnorderedSet<T, M>::emplace(Key k) {
  set_.insert(std::move(k));
}

template <typename T, typename M>
inline GeneralUnorderedSet<T, M>::ConstIterator GeneralUnorderedSet<T, M>::find(
    Key k) const {
  return set_.find(std::move(k));
}

template <typename T, typename M>
inline void GeneralUnorderedSet<T, M>::merge(GeneralUnorderedSet s) {
  set_.merge(std::move(s.set_));
}

template <typename T, typename M>
template <typename... S0s, typename... S1s>
inline void GeneralUnorderedSet<T, M>::for_all(void (*fn)(const Key &key,
                                                          S0s...),
                                               S1s &&... states) {
  // Not implemented.
  BUG();
}

template <typename T, typename M>
inline GeneralUnorderedSet<T, M>::GeneralUnorderedSet(USet initial_state)
    : set_(std::move(initial_state)) {}

template <typename T, typename M>
void GeneralUnorderedSet<T, M>::split(Key *mid_k,
                                      GeneralUnorderedSet *latter_half) {
  using Pair = std::pair<T, typename USet::iterator>;

  std::vector<Pair> keys;
  keys.reserve(set_.size());
  for (auto iter = set_.begin(); iter != set_.end(); ++iter) {
    keys.emplace_back(*iter, iter);
  }
  std::nth_element(
      keys.begin(), keys.begin() + keys.size() / 2, keys.end(),
      [](const Pair &x, const Pair &y) { return x.first < y.first; });
  *mid_k = keys[keys.size() / 2].first;

  for (std::size_t i = keys.size() / 2; i < keys.size(); i++) {
    auto node = set_.extract(keys[i].second);
    latter_half->set_.insert(std::move(node));
  }
}

template <typename T, typename M>
inline GeneralUnorderedSet<T, M>::USet &GeneralUnorderedSet<T, M>::data() {
  return set_;
}

template <typename T, typename M>
inline GeneralUnorderedSet<T, M>::ConstIterator
GeneralUnorderedSet<T, M>::cbegin() const {
  return set_.cbegin();
}

template <typename T, typename M>
inline GeneralUnorderedSet<T, M>::ConstIterator
GeneralUnorderedSet<T, M>::cend() const {
  return set_.cend();
}

template <typename T, typename M>
template <class Archive>
inline void GeneralUnorderedSet<T, M>::save(Archive &ar) const {
  ar(set_);
}

template <typename T, typename M>
template <class Archive>
inline void GeneralUnorderedSet<T, M>::load(Archive &ar) {
  ar(set_);
}

template <typename T, typename M, typename LL>
inline GeneralShardedUnorderedSet<T, M, LL>::GeneralShardedUnorderedSet(
    std::optional<typename Base::Hint> hint)
    : Base(hint) {}

template <typename T, typename LL>
inline ShardedUnorderedSet<T, LL> make_sharded_unordered_set() {
  return ShardedUnorderedSet<T, LL>(std::nullopt);
}

template <typename T, typename LL>
inline ShardedUnorderedMultiSet<T, LL> make_sharded_unordered_multiset() {
  return ShardedUnorderedMultiSet<T, LL>(std::nullopt);
}

}  // namespace nu
