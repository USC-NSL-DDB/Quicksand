#pragma once

#include <set>

#include "sharded_ds.hpp"

namespace nu {
template <class Set>
struct SetConstIterator : public Set::const_iterator {
  constexpr static bool kContiguous = false;

  SetConstIterator();
  SetConstIterator(Set::iterator &&iter);
};

template <class Set>
struct SetConstReverseIterator : public Set::const_reverse_iterator {
  constexpr static bool kContiguous = false;

  SetConstReverseIterator();
  SetConstReverseIterator(Set::reverse_iterator &&iter);
};

template <typename T, BoolIntegral M>
class GeneralSet {
 public:
  using Key = T;
  using Set = std::conditional_t<M::value, std::multiset<T>, std::set<T>>;
  using ConstIterator = SetConstIterator<Set>;
  using ConstReverseIterator = SetConstReverseIterator<Set>;

  GeneralSet();
  GeneralSet(const GeneralSet &) = default;
  GeneralSet &operator=(const GeneralSet &) = default;
  GeneralSet(GeneralSet &&) noexcept = default;
  GeneralSet &operator=(GeneralSet &&) noexcept = default;

  std::size_t size() const;
  bool empty() const;
  void clear();
  std::size_t insert(Key k);
  ConstIterator find(Key k) const;
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, S0s...), S1s &&... states);
  void split(Key *mid_k, GeneralSet *latter_half);
  void merge(GeneralSet s);
  ConstIterator cbegin() const;
  ConstIterator cend() const;
  ConstReverseIterator crbegin() const;
  ConstReverseIterator crend() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);
  Set &data();

 private:
  GeneralSet(Set initial_state);

  Set set_;
};

template <typename T, BoolIntegral M, typename LL>
class GeneralShardedSet;

template <typename T, typename LL>
using ShardedSet = GeneralShardedSet<T, std::false_type, LL>;

template <typename T, typename LL>
using ShardedMultiSet = GeneralShardedSet<T, std::true_type, LL>;

template <typename T, BoolIntegral M, typename LL>
class GeneralShardedSet;

template <typename T, BoolIntegral M, typename LL>
class GeneralShardedSet
    : public ShardedDataStructure<GeneralLockedContainer<GeneralSet<T, M>>,
                                  LL> {
 public:
  GeneralShardedSet(const GeneralShardedSet &) = default;
  GeneralShardedSet &operator=(const GeneralShardedSet &) = default;
  GeneralShardedSet(GeneralShardedSet &&) noexcept = default;
  GeneralShardedSet &operator=(GeneralShardedSet &&) noexcept = default;

 private:
  using Base =
      ShardedDataStructure<GeneralLockedContainer<GeneralSet<T, M>>, LL>;
  GeneralShardedSet() = default;
  GeneralShardedSet(std::optional<typename Base::ShardingHint> sharding_hint);

  friend class ProcletServer;
  template <typename T1, typename LL1>
  friend ShardedSet<T1, LL1> make_sharded_set();
  template <typename T1, typename LL1>
  friend ShardedMultiSet<T1, LL1> make_sharded_multi_set();
};

template <typename T, typename LL>
ShardedSet<T, LL> make_sharded_set();

template <typename T, typename LL>
ShardedMultiSet<T, LL> make_sharded_multi_set();

}  // namespace nu

#include "nu/impl/sharded_set.ipp"
