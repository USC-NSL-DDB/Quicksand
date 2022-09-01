#pragma once

#include <set>

#include "sharded_ds.hpp"

namespace nu {
template <typename T>
struct SetConstIterator : public std::set<T>::const_iterator {
  SetConstIterator();
  SetConstIterator(std::set<T>::const_iterator &&iter);
  template <class Archive>
  void serialize(Archive &ar);
};

template <typename T>
struct SetConstReverseIterator : public std::set<T>::const_reverse_iterator {
  SetConstReverseIterator();
  SetConstReverseIterator(std::set<T>::const_reverse_iterator &&iter);
  template <class Archive>
  void serialize(Archive &ar);
};

template <typename T>
class Set {
 public:
  using Key = T;
  using Val = T;
  using IterVal = T;
  using ConstIterator = SetConstIterator<T>;
  using ConstReverseIterator = SetConstReverseIterator<T>;

  Set();
  Set(std::size_t capacity);
  Set(const Set &) = default;
  Set &operator=(const Set &) = default;
  Set(Set &&) noexcept = default;
  Set &operator=(Set &&) noexcept = default;

  std::size_t size() const;
  bool empty() const;
  void clear();
  void emplace(Key k, Val v);
  void emplace_batch(Set s);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  std::pair<Key, Set> split();
  ConstIterator cbegin() const;
  ConstIterator cend() const;
  ConstReverseIterator crbegin() const;
  ConstReverseIterator crend() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);
  std::set<T> &data();

 private:
  std::set<T> set_;
};

template <typename T, typename LL>
class ShardedSet
    : public ShardedDataStructure<GeneralLockedContainer<Set<T>>, LL> {
 public:
  ShardedSet() = default;
  ShardedSet(const ShardedSet &) = default;
  ShardedSet &operator=(const ShardedSet &) = default;
  ShardedSet(ShardedSet &&) noexcept = default;
  ShardedSet &operator=(ShardedSet &&) noexcept = default;

  void insert(const T &value);
  bool empty();

 private:
  using Base = ShardedDataStructure<GeneralLockedContainer<Set<T>>, LL>;
  ShardedSet(std::optional<typename Base::Hint> hint);

  template <typename T1, typename LL1>
  friend ShardedSet<T1, LL1> make_sharded_set();
};

template <typename T, typename LL>
ShardedSet<T, LL> make_sharded_set();
}  // namespace nu

#include "nu/impl/sharded_set.ipp"
