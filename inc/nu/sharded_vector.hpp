#pragma once

#include <vector>

#include "sharded_ds.hpp"

namespace nu {

template <typename T>
struct VectorConstIterator : public std::vector<T>::const_iterator {
  VectorConstIterator();
  VectorConstIterator(std::vector<T>::const_iterator &&iter);
  template <class Archive>
  void serialize(Archive &ar);
};

template <typename T>
struct VectorConstReverseIterator
    : public std::vector<T>::const_reverse_iterator {
  VectorConstReverseIterator();
  VectorConstReverseIterator(std::vector<T>::const_reverse_iterator &&iter);
  template <class Archive>
  void serialize(Archive &ar);
};

template <typename T>
class Vector {
 public:
  using Key = std::size_t;
  using Val = T;
  using IterVal = T;
  using ConstIterator = VectorConstIterator<T>;
  using ConstReverseIterator = VectorConstReverseIterator<T>;

  Vector();
  Vector(std::size_t capacity);
  Vector(const Vector &);
  Vector &operator=(const Vector &);
  Vector(Vector &&) noexcept;
  Vector &operator=(Vector &&) noexcept;

  std::size_t size() const;
  std::size_t capacity() const;
  bool empty() const;
  void clear();
  void emplace(Key k, Val v);
  void emplace_batch(Vector &&vector);
  std::optional<T> find_val(Key k);
  std::pair<Key, Vector> split();
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
  std::vector<T> data_;
  Key l_key_;
};

template <typename T, typename LL>
class ShardedVector
    : public ShardedDataStructure<GeneralLockedContainer<Vector<T>>, LL> {
 public:
  ShardedVector();
  ShardedVector(const ShardedVector &) = default;
  ShardedVector &operator=(const ShardedVector &) = default;
  ShardedVector(ShardedVector &&) noexcept = default;
  ShardedVector &operator=(ShardedVector &&) noexcept = default;

  T operator[](std::size_t index);
  void push_back(const T &value);
  void pop_back();
  std::size_t size();
  bool empty();
  void clear();

 private:
  using Base = ShardedDataStructure<GeneralLockedContainer<Vector<T>>, LL>;
  std::size_t size_;

  ShardedVector(std::optional<typename Base::Hint> hint);
  template <typename T1, typename LL1>
  friend ShardedVector<T1, LL1> make_sharded_vector();
};

template <typename T, typename LL>
ShardedVector<T, LL> make_sharded_vector();

}  // namespace nu

#include "nu/impl/sharded_vector.ipp"
