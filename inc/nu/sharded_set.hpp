#pragma once

#include "sharded_ds.hpp"

namespace nu {

template <typename T>
class Set {
 public:
  using Key = T;
  using Val = T;
  using IterVal = T;
  // TODO
  using ConstIterator = std::tuple<>;
  using ConstReverseIterator = std::tuple<>;

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
  void emplace_batch(Set &&s);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  std::pair<Key, Set> split();
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
