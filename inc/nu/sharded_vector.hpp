#pragma once

#include <functional>
#include <vector>

#include "sharded_ds.hpp"

namespace nu {

template <typename T>
struct VectorConstIterator : public std::vector<T>::const_iterator {
  VectorConstIterator();
  VectorConstIterator(std::vector<T>::iterator &&iter);
  VectorConstIterator(std::vector<T>::const_iterator &&iter);
};

template <typename T>
struct VectorConstReverseIterator
    : public std::vector<T>::const_reverse_iterator {
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

  constexpr static float kDefaultGrowthFactor = 2.0;

  Vector();
  Vector(std::size_t capacity);
  Vector(const Vector &);
  Vector &operator=(const Vector &);
  Vector(Vector &&) noexcept;
  Vector &operator=(Vector &&) noexcept;

  std::size_t size() const;
  std::size_t capacity() const;
  void reserve(std::size_t size);
  void set_max_growth_factor_fn(const std::function<float()> &fn);
  bool empty() const;
  void clear();
  void emplace(Key k, Val v);
  void emplace_back(Val v);
  void emplace_back_batch(std::vector<Val> v);
  ConstIterator find(Key k);
  std::pair<Key, Vector> split();
  void merge(Vector vector);
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
  std::function<float()> max_growth_factor_fn_;
  bool has_split_;
};

template <typename T, typename LL>
class ShardedVector
    : public ShardedDataStructure<GeneralLockedContainer<Vector<T>>, LL> {
 public:
  ShardedVector(const ShardedVector &) = default;
  ShardedVector &operator=(const ShardedVector &) = default;
  ShardedVector(ShardedVector &&) noexcept = default;
  ShardedVector &operator=(ShardedVector &&) noexcept = default;

  T operator[](std::size_t index) const;
  void set(std::size_t index, T value);
  void push_back(const T &value);
  void emplace_back(T &&value);
  void pop_back();

 private:
  using Base = ShardedDataStructure<GeneralLockedContainer<Vector<T>>, LL>;

  ShardedVector();
  ShardedVector(std::optional<typename Base::Hint> hint);
  friend class ProcletServer;
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
