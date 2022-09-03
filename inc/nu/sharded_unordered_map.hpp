#pragma once

#include "sharded_ds.hpp"

namespace nu {
template <typename K, typename V>
struct UnorderedMapConstIterator
    : public std::unordered_map<K, V>::const_iterator {
  UnorderedMapConstIterator();
  UnorderedMapConstIterator(std::unordered_map<K, V>::const_iterator &&iter);
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

template <typename K, typename V>
class UnorderedMap {
 public:
  using Key = K;
  using Val = V;
  using IterVal = std::pair<K, V>;
  using ConstIterator = UnorderedMapConstIterator<K, V>;
  using ConstReverseIterator = NoopIterator<IterVal>;

  UnorderedMap() = default;
  UnorderedMap(std::size_t capacity);
  UnorderedMap(const UnorderedMap &) = default;
  UnorderedMap &operator=(const UnorderedMap &) = default;
  UnorderedMap(UnorderedMap &&) noexcept = default;
  UnorderedMap &operator=(UnorderedMap &&) noexcept = default;

  std::size_t size() const;
  bool empty() const;
  void clear();
  void emplace(Key k, Val v);
  void emplace_back(Val v);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  std::optional<Val> find_val(Key k);
  std::pair<Key, UnorderedMap> split();
  void merge(UnorderedMap m);
  ConstIterator cbegin() const;
  ConstIterator cend() const;
  ConstReverseIterator crbegin() const;
  ConstReverseIterator crend() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  UnorderedMap(std::unordered_map<K, V> initial_state);

  std::unordered_map<K, V> map_;
};

template <typename K, typename V, typename LL>
class ShardedUnorderedMap
    : public ShardedDataStructure<GeneralLockedContainer<UnorderedMap<K, V>>,
                                  LL> {
 public:
  ShardedUnorderedMap(const ShardedUnorderedMap &) = default;
  ShardedUnorderedMap &operator=(const ShardedUnorderedMap &) = default;
  ShardedUnorderedMap(ShardedUnorderedMap &&) noexcept = default;
  ShardedUnorderedMap &operator=(ShardedUnorderedMap &&) noexcept = default;

  V operator[](const K &);

 private:
  using Base =
      ShardedDataStructure<GeneralLockedContainer<UnorderedMap<K, V>>, LL>;
  ShardedUnorderedMap() = default;
  ShardedUnorderedMap(std::optional<typename Base::Hint> hint);

  friend class ProcletServer;
  template <typename K1, typename V1, typename LL1>
  friend ShardedUnorderedMap<K1, V1, LL1> make_sharded_unordered_map();
};

template <typename K, typename V, typename LL>
ShardedUnorderedMap<K, V, LL> make_sharded_unordered_map();
}  // namespace nu

#include "nu/impl/sharded_unordered_map.ipp"
