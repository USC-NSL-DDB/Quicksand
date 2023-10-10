#pragma once

#include <functional>
#include <unordered_set>

#include "sharded_ds.hpp"

namespace nu {

template <class USet>
struct UnorderedSetConstIterator : public USet::const_iterator {
  constexpr static bool kContiguous = false;

  UnorderedSetConstIterator();
  UnorderedSetConstIterator(USet::iterator &&iter);
  UnorderedSetConstIterator(USet::const_iterator &&iter);
};

template <typename T, BoolIntegral M>
class GeneralUnorderedSet {
 public:
  using Key = T;
  using USet = std::conditional_t<M::value, std::unordered_multiset<T>,
                                  std::unordered_set<T>>;
  using ConstIterator = UnorderedSetConstIterator<USet>;

  GeneralUnorderedSet() = default;
  GeneralUnorderedSet(const GeneralUnorderedSet &) = default;
  GeneralUnorderedSet &operator=(const GeneralUnorderedSet &) = default;
  GeneralUnorderedSet(GeneralUnorderedSet &&) noexcept = default;
  GeneralUnorderedSet &operator=(GeneralUnorderedSet &&) noexcept = default;

  std::size_t size() const;
  void reserve(std::size_t size);
  bool empty() const;
  void clear();
  std::size_t insert(Key k);
  ConstIterator find(Key k) const;
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, S0s...), S1s &&...states);
  void split(Key *mid_k, GeneralUnorderedSet *latter_half);
  void merge(GeneralUnorderedSet s);
  ConstIterator cbegin() const;
  ConstIterator cend() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);
  USet &data();

 private:
  GeneralUnorderedSet(USet initial_state);

  USet set_;
};

template <typename T, BoolIntegral M, typename LL>
class GeneralShardedUnorderedSet;

template <typename T, typename LL>
using ShardedUnorderedSet = GeneralShardedUnorderedSet<T, std::false_type, LL>;

template <typename T, typename LL>
using ShardedUnorderedMultiSet =
    GeneralShardedUnorderedSet<T, std::true_type, LL>;

template <typename T, BoolIntegral M, typename LL>
class GeneralShardedUnorderedSet
    : public ShardedDataStructure<
          GeneralLockedContainer<GeneralUnorderedSet<T, M>>, LL> {
 public:
  GeneralShardedUnorderedSet() = default;
  GeneralShardedUnorderedSet(const GeneralShardedUnorderedSet &) = default;
  GeneralShardedUnorderedSet &operator=(const GeneralShardedUnorderedSet &) =
      default;
  GeneralShardedUnorderedSet(GeneralShardedUnorderedSet &&) noexcept = default;
  GeneralShardedUnorderedSet &operator=(
      GeneralShardedUnorderedSet &&) noexcept = default;

 private:
  using Base =
      ShardedDataStructure<GeneralLockedContainer<GeneralUnorderedSet<T, M>>,
                           LL>;
  GeneralShardedUnorderedSet(
      std::optional<typename Base::ShardingHint> sharding_hint);

  friend class ProcletServer;
  template <typename T1, typename LL1>
  friend ShardedUnorderedSet<T1, LL1> make_sharded_unordered_set();
  template <typename T1, typename LL1>
  friend ShardedUnorderedMultiSet<T1, LL1> make_sharded_unordered_multiset();
};

template <typename T, typename LL>
ShardedUnorderedSet<T, LL> make_sharded_unordered_set();

template <typename T, typename LL>
ShardedUnorderedMultiSet<T, LL> make_sharded_unordered_multiset();

}  // namespace nu

#include "nu/impl/sharded_unordered_set.ipp"
