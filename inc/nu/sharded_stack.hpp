#pragma once

#include <stack>

#include "sharded_ds.hpp"

namespace nu {

template <typename T>
class Stack {
 public:
  using Key = std::size_t;
  using Val = T;

  Stack();
  Stack(const Stack &) = default;
  Stack &operator=(const Stack &) = default;
  Stack(Stack &&) noexcept = default;
  Stack &operator=(Stack &&) noexcept = default;

  std::size_t size() const;
  bool empty() const;
  Val back() const;
  void emplace_back(Val v);
  void emplace_back_batch(std::vector<Val> v);
  void pop_back();
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states);
  void split(Key *mid_k, Stack *latter_half);
  void merge(Stack stack);
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  std::stack<T> stack_;
  Key l_key_;
};

template <typename T, typename LL>
class ShardedStack
    : public ShardedDataStructure<GeneralLockedContainer<Stack<T>>, LL> {
 public:
  ShardedStack(const ShardedStack &) = default;
  ShardedStack &operator=(const ShardedStack &) = default;
  ShardedStack(ShardedStack &&) noexcept = default;
  ShardedStack &operator=(ShardedStack &&) noexcept = default;

  void push(const T &value);
  T top() const;
  void pop();

 private:
  using Base = ShardedDataStructure<GeneralLockedContainer<Stack<T>>, LL>;

  ShardedStack();
  ShardedStack(std::optional<typename Base::Hint> hint);
  friend class ProcletServer;
  template <typename T1, typename LL1>
  friend ShardedStack<T1, LL1> make_sharded_stack();
};

template <typename T, typename LL>
ShardedStack<T, LL> make_sharded_stack();

}  // namespace nu

#include "nu/impl/sharded_stack.ipp"
