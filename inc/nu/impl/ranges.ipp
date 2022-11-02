namespace nu {

template <typename It>
Range<It>::Range() {}

template <typename It>
Range<It>::Range(const It &begin, const It &end) : curr_(begin), end_(end) {}

template <typename It>
Range<It> &Range<It>::operator++() {
  ++curr_;
  return *this;
}

template <typename It>
const It::IterVal &Range<It>::operator*() {
  return *curr_;
}

template <typename It>
It Range<It>::begin() const {
  return curr_;
}

template <typename It>
It Range<It>::end() const {
  return end_;
}

template <typename It>
template <class Archive>
inline void Range<It>::save(Archive &ar) const {
  ar(curr_, end_);
}

template <typename It>
template <class Archive>
inline void Range<It>::load(Archive &ar) {
  ar(curr_, end_);
}

template <typename T>
Range<GeneralSealedDSConstIterator<T, true>> range(const SealedDS<T> &sealed) {
  return Range(sealed.cbegin(), sealed.cend());
}

template <typename... Its>
ZippedIterator<Its...>::ZippedIterator(const Its &... iters)
    : iters_(iters...) {}

template <typename... Its>
ZippedIterator<Its...>::ZippedIterator(Its &&... iters)
    : iters_(std::forward<Its>(iters)...) {}

template <typename... Its>
const ZippedIterator<Its...>::IterVal ZippedIterator<Its...>::operator*() {
  return std::apply([](auto &&... iters) { return std::tie(*iters...); },
                    iters_);
}

template <typename... Its>
ZippedIterator<Its...> &ZippedIterator<Its...>::operator++() {
  std::apply([](auto &&... iter) { ((++iter), ...); }, iters_);
  return *this;
}

template <typename... Its>
bool ZippedIterator<Its...>::operator!=(const ZippedIterator &other) {
  return iters_ == other.iters_;
}

template <typename... Rs>
Zip<Rs...>::Zip(const Rs &... ranges)
    : begin_((ranges.begin())...), end_((ranges.end())...) {}

template <typename... Rs>
Zip<Rs...>::Iter Zip<Rs...>::begin() const {
  return begin_;
}

template <typename... Rs>
Zip<Rs...>::Iter Zip<Rs...>::end() const {
  return end_;
}

template <typename... Ts>
Zip<Range<GeneralSealedDSConstIterator<Ts, true>>...> zip(
    const SealedDS<Ts> &... sealed_ds) {
  return Zip(range(sealed_ds)...);
}
}  // namespace nu
