#pragma once

#include <unordered_set>

#include "sharded_ds.hpp"

namespace nu {
template <typename T>
struct UnorderedSetConstIterator
    : public std::unordered_set<T>::const_iterator {
  UnorderedSetConstIterator();
  UnorderedSetConstIterator(std::unordered_set<T>::const_iterator &&iter);
  template <class Archive>
  void serialize(Archive &ar);
};

template <typename IterVal>
struct NoopIterator {
  NoopIterator(){};
  IterVal operator*() {
    BUG();
    return IterVal();
  };
  bool operator==(NoopIterator &rhs) { return true; };
  void operator++(int incr){};
};

template <typename T>
class UnorderedSet {
 public:
  using Key = T;
  using Val = T;
  using IterVal = T;
  using ConstIterator = UnorderedSetConstIterator<T>;
  using ConstReverseIterator = NoopIterator<T>;

  UnorderedSet();
  UnorderedSet(std::size_t capacity);
  UnorderedSet(const UnorderedSet &) = default;
  UnorderedSet &operator=(const UnorderedSet &) = default;
  UnorderedSet(UnorderedSet &&) noexcept = default;
  UnorderedSet &operator=(UnorderedSet &&) noexcept = default;

  std::size_t size() const;
  bool empty() const;
  void clear();
  void emplace(Key k, Val v);
  void emplace_back(Val v);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  std::pair<Key, UnorderedSet> split();
  void merge(UnorderedSet s);
  ConstIterator cbegin() const;
  ConstIterator cend() const;
  ConstReverseIterator crbegin() const;
  ConstReverseIterator crend() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);
  std::unordered_set<T> &data();

 private:
  std::unordered_set<T> set_;
};

template <typename T, typename LL>
class ShardedUnorderedSet
    : public ShardedDataStructure<GeneralLockedContainer<UnorderedSet<T>>, LL> {
 public:
  ShardedUnorderedSet(const ShardedUnorderedSet &) = default;
  ShardedUnorderedSet &operator=(const ShardedUnorderedSet &) = default;
  ShardedUnorderedSet(ShardedUnorderedSet &&) noexcept = default;
  ShardedUnorderedSet &operator=(ShardedUnorderedSet &&) noexcept = default;

  void insert(const T &value);
  bool empty();

 private:
  using Base =
      ShardedDataStructure<GeneralLockedContainer<UnorderedSet<T>>, LL>;
  ShardedUnorderedSet() = default;
  ShardedUnorderedSet(std::optional<typename Base::Hint> hint);

  friend class ProcletServer;
  template <typename T1, typename LL1>
  friend ShardedUnorderedSet<T1, LL1> make_sharded_unordered_set();
};

template <typename T, typename LL>
ShardedUnorderedSet<T, LL> make_sharded_unordered_set();
}  // namespace nu

#include "nu/impl/sharded_unordered_set.ipp"
