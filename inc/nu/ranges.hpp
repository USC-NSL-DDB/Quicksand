#include "nu/sealed_ds.hpp"

namespace nu {

template <typename It>
class Range {
 public:
  Range();
  Range(const It &begin, const It &end);
  Range(const Range &) = default;
  Range &operator=(const Range &) = default;
  Range(Range &&) = default;
  Range &operator=(Range &&) = default;
  Range &operator++();
  const It::IterVal &operator*();
  It begin() const;
  It end() const;

  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  It curr_;
  It end_;
};

template <typename... Its>
class ZippedIterator {
 public:
  using IterVal = std::tuple<iter_val_t<Its>...>;

  ZippedIterator(Its &&... iters);
  ZippedIterator(const ZippedIterator &) = default;
  ZippedIterator &operator=(const ZippedIterator &) = default;
  ZippedIterator(ZippedIterator &&) = default;
  ZippedIterator &operator=(ZippedIterator &&) = default;
  ZippedIterator &operator++();
  bool operator!=(const ZippedIterator &);
  const IterVal operator*();

 private:
  std::tuple<Its...> iters_;
};

template <typename... Rs>
class Zip {
 public:
  using Iter = ZippedIterator<range_iter_t<Rs>...>;

  Zip(const Rs &... ranges);
  Zip(const Zip &) = default;
  Zip &operator=(const Zip &) = default;
  Zip(Zip &&) = default;
  Zip &operator=(Zip &&) = default;

  Iter begin() const;
  Iter end() const;

 private:
  Iter begin_;
  Iter end_;
};

template <typename T>
Range<GeneralSealedDSConstIterator<T, true>> range(const SealedDS<T> &sealed);

template <typename... Ts>
Zip<Range<GeneralSealedDSConstIterator<Ts, true>>...> zip(
    const SealedDS<Ts> &... sealed_ds);
}  // namespace nu

#include "nu/impl/ranges.ipp"
