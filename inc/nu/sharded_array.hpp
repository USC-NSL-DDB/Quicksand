#pragma once

#include <vector>

#include "sharded_ds.hpp"

namespace nu {
template <typename T>
struct ArrayConstIterator : public std::vector<T>::const_iterator {
  ArrayConstIterator();
  ArrayConstIterator(std::vector<T>::iterator &&iter);
  ArrayConstIterator(std::vector<T>::const_iterator &&iter);
  template <class Archive>
  void serialize(Archive &ar);
};

template <typename T>
struct ArrayConstReverseIterator
    : public std::vector<T>::const_reverse_iterator {
  ArrayConstReverseIterator();
  ArrayConstReverseIterator(std::vector<T>::reverse_iterator &&iter);
  ArrayConstReverseIterator(std::vector<T>::const_reverse_iterator &&iter);
  template <class Archive>
  void serialize(Archive &ar);
};

template <typename T>
class Array {
 public:
  using Key = std::size_t;
  using Val = T;
  using ConstIterator = ArrayConstIterator<T>;
  using ConstReverseIterator = ArrayConstReverseIterator<T>;

  Array();
  Array(std::optional<Key> l_key);
  Array(std::optional<Key> l_key, std::size_t size);
  Array(const Array &);
  Array &operator=(const Array &);
  Array(Array &&) noexcept;
  Array &operator=(Array &&) noexcept;

  std::size_t size() const;
  std::size_t capacity() const;
  bool empty() const;
  void clear();
  void emplace(Key k, Val v);
  ConstIterator find(Key k);
  std::pair<Key, Array> split();
  void merge(Array arr);
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  ConstIterator cbegin() const;
  ConstIterator cend() const;
  ConstReverseIterator crbegin() const;
  ConstReverseIterator crend() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  std::size_t l_key_;
  std::vector<T> data_;
};

template <typename T, typename LL>
class ShardedArray
    : public ShardedDataStructure<GeneralLockedContainer<Array<T>>, LL> {
 public:
  ShardedArray(const ShardedArray &) = default;
  ShardedArray &operator=(const ShardedArray &) = default;
  ShardedArray(ShardedArray &&) noexcept = default;
  ShardedArray &operator=(ShardedArray &&) noexcept = default;

  T operator[](std::size_t index) const;
  void set(std::size_t index, T value);

 private:
  using Base = ShardedDataStructure<GeneralLockedContainer<Array<T>>, LL>;

  ShardedArray();
  ShardedArray(std::optional<typename Base::Hint> hint);
  friend class ProcletServer;
  template <typename T1, typename LL1>
  friend ShardedArray<T1, LL1> make_sharded_array(std::size_t size);
};

template <typename T, typename LL>
ShardedArray<T, LL> make_sharded_array(std::size_t size);
}  // namespace nu

#include "nu/impl/sharded_array.ipp"
