#pragma once

#include "sharded_ds.hpp"

namespace nu {

template <typename T>
class Set {
 public:
  using Key = T;
  using Val = T;

  Set();
  Set(std::size_t capacity);
  Set(const Set &);
  Set &operator=(const Set &);
  Set(Set &&) noexcept;
  Set &operator=(Set &&) noexcept;

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

template <typename T, bool LowLat>
class ShardedSet
    : public ShardedDataStructure<GeneralContainer<Set<T>>, LowLat> {
 public:
  ShardedSet() = default;
  ShardedSet(const ShardedSet &) = default;
  ShardedSet &operator=(const ShardedSet &) = default;
  ShardedSet(ShardedSet &&) noexcept = default;
  ShardedSet &operator=(ShardedSet &&) noexcept = default;

  void insert(const T &value);
  bool empty();

 private:
  using Base = ShardedDataStructure<GeneralContainer<Set<T>>, LowLat>;
  ShardedSet(std::optional<typename Base::Hint> hint);

  template <typename T1, bool LL1>
  friend ShardedSet<T1, LL1> make_sharded_set();
};

template <typename T, bool LowLat>
ShardedSet<T, LowLat> make_sharded_set();
}  // namespace nu

#include "nu/impl/sharded_set.ipp"
