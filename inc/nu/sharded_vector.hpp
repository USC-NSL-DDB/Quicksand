#pragma once

#include <functional>
#include <vector>

#include "sealed_ds.hpp"
#include "sharded_ds.hpp"

namespace nu {

template <typename T>
struct VectorConstIterator : public std::vector<T>::const_iterator {
  constexpr static bool kContiguous = true;

  VectorConstIterator();
  VectorConstIterator(std::vector<T>::iterator &&iter);
  VectorConstIterator(std::vector<T>::const_iterator &&iter);
};

template <typename T>
struct VectorConstReverseIterator
    : public std::vector<T>::const_reverse_iterator {
  constexpr static bool kContiguous = true;

  VectorConstReverseIterator();
  VectorConstReverseIterator(std::vector<T>::reverse_iterator &&iter);
  VectorConstReverseIterator(std::vector<T>::const_reverse_iterator &&iter);
};

template <typename T>
class Vector {
 public:
  using Key = std::size_t;
  using Val = T;
  using ConstIterator = VectorConstIterator<T>;
  using ConstReverseIterator = VectorConstReverseIterator<T>;

  Vector();
  Vector(const Vector &);
  Vector &operator=(const Vector &);
  Vector(Vector &&) noexcept;
  Vector &operator=(Vector &&) noexcept;

  std::size_t size() const;
  std::size_t capacity() const;
  void reserve(std::size_t size);
  bool empty() const;
  void clear();
  std::size_t insert(Key k, Val v);
  std::size_t push_back(Val v);
  std::size_t push_back_batch(std::vector<Val> v);
  ConstIterator find(Key k) const;
  void split(Key *mid_k, Vector *latter_half);
  void merge(Vector vector);
  Key rebase(Key new_l_key);
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
  std::vector<T> &data();

 private:
  std::vector<T> data_;
  Key l_key_;
  bool has_split_;
};

template <typename T, typename LL>
class ShardedVector;

template <typename T, typename LL>
class ShardedVector
    : public ShardedDataStructure<GeneralLockedContainer<Vector<T>>, LL> {
 public:
  ShardedVector();
  ShardedVector(const ShardedVector &) = default;
  ShardedVector &operator=(const ShardedVector &) = default;
  ShardedVector(ShardedVector &&) noexcept = default;
  ShardedVector &operator=(ShardedVector &&) noexcept = default;

  T operator[](std::size_t index) const;
  void set(std::size_t index, T value);

 private:
  using Base = ShardedDataStructure<GeneralLockedContainer<Vector<T>>, LL>;

  ShardedVector(std::optional<typename Base::Hint> hint);
  friend class ProcletServer;
  template <typename T1, typename LL1>
  friend class VectorBackInserter;
  template <typename T1, typename LL1>
  friend ShardedVector<T1, LL1> make_sharded_vector();
  template <typename T1, typename LL1>
  friend ShardedVector<T1, LL1> make_sharded_vector(uint64_t reserved_count);
};

template <typename T, typename LL>
ShardedVector<T, LL> make_sharded_vector();

template <typename T, typename LL>
ShardedVector<T, LL> make_sharded_vector(uint64_t reserved_count);

}  // namespace nu

#include "nu/impl/sharded_vector.ipp"
