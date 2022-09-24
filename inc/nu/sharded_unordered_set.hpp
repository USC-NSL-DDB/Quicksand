#pragma once

#include <unordered_set>

#include "sharded_ds.hpp"

namespace nu {

template <class USet>
struct UnorderedSetConstIterator : public USet::const_iterator {
  UnorderedSetConstIterator();
  UnorderedSetConstIterator(USet::iterator &&iter);
  UnorderedSetConstIterator(USet::const_iterator &&iter);
  template <class Archive>
  void serialize(Archive &ar);
};

template <typename T, BoolIntegral M>
class GeneralUnorderedSet {
 public:
  using Key = T;
  using Val = T;
  using USet = std::conditional_t<M::value, std::unordered_multiset<T>,
                                  std::unordered_set<T>>;
  using ConstIterator = UnorderedSetConstIterator<USet>;

  GeneralUnorderedSet() = default;
  GeneralUnorderedSet(std::optional<Key> l_key);
  GeneralUnorderedSet(std::optional<Key> l_key, std::size_t capacity);
  GeneralUnorderedSet(const GeneralUnorderedSet &) = default;
  GeneralUnorderedSet &operator=(const GeneralUnorderedSet &) = default;
  GeneralUnorderedSet(GeneralUnorderedSet &&) noexcept = default;
  GeneralUnorderedSet &operator=(GeneralUnorderedSet &&) noexcept = default;

  std::size_t size() const;
  bool empty() const;
  void clear();
  void emplace(Key k, Val v);
  ConstIterator find(Key k);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  std::pair<Key, GeneralUnorderedSet> split();
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
  GeneralShardedUnorderedSet(const GeneralShardedUnorderedSet &) = default;
  GeneralShardedUnorderedSet &operator=(const GeneralShardedUnorderedSet &) =
      default;
  GeneralShardedUnorderedSet(GeneralShardedUnorderedSet &&) noexcept = default;
  GeneralShardedUnorderedSet &operator=(
      GeneralShardedUnorderedSet &&) noexcept = default;
  void insert(const T &value);
  bool empty();

 private:
  using Base =
      ShardedDataStructure<GeneralLockedContainer<GeneralUnorderedSet<T, M>>,
                           LL>;
  GeneralShardedUnorderedSet() = default;
  GeneralShardedUnorderedSet(std::optional<typename Base::Hint> hint);

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
