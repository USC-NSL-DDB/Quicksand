#pragma once

#include "nu/sharded_ds.hpp"
#include "nu/utils/splitmix64.hpp"

namespace nu {

template <typename T>
class ShardedService
    : private ShardedDataStructure<GeneralContainer<T>, std::false_type> {
 public:
  using Key = T::Key;

  template <typename RetT, typename... S0s, typename... S1s>
  Future<RetT> run_async(Key k, RetT (*fn)(T &, S0s...),
                         S1s &&...states) requires
      ValidInvocationTypes<RetT, S0s...>;
  template <typename RetT, typename... S0s, typename... S1s>
  RetT run(Key k, RetT (*fn)(T &, S0s...),
           S1s &&...states) requires ValidInvocationTypes<RetT, S0s...>;
  template <typename RetT, typename... A0s, typename... A1s>
  Future<RetT> run_async(Key k, RetT (T::*md)(A0s...), A1s &&...args) requires
      ValidInvocationTypes<RetT, A0s...>;
  template <typename RetT, typename... A0s, typename... A1s>
  RetT run(Key k, RetT (T::*md)(A0s...),
           A1s &&...args) requires ValidInvocationTypes<RetT, A0s...>;

 private:
  using Base = ShardedDataStructure<GeneralContainer<T>, std::false_type>;

  ShardedService();
  template <typename U, typename... As>
  friend ShardedService<U> make_sharded_service(As &&...args);
};

template <typename T>
class ShardedStatelessService : private ShardedService<T> {
 public:
  ShardedStatelessService(const ShardedStatelessService<T> &);
  ShardedStatelessService(ShardedStatelessService<T> &&) = default;
  ShardedStatelessService &operator=(const ShardedStatelessService<T> &);
  ShardedStatelessService &operator=(ShardedStatelessService<T> &&) = default;

  template <typename RetT, typename... S0s, typename... S1s>
  Future<RetT> run_async(RetT (*fn)(T &, S0s...), S1s &&...states) requires
      ValidInvocationTypes<RetT, S0s...>;
  template <typename RetT, typename... S0s, typename... S1s>
  RetT run(RetT (*fn)(T &, S0s...),
           S1s &&...states) requires ValidInvocationTypes<RetT, S0s...>;
  template <typename RetT, typename... A0s, typename... A1s>
  Future<RetT> run_async(RetT (T::*md)(A0s...), A1s &&...args) requires
      ValidInvocationTypes<RetT, A0s...>;
  template <typename RetT, typename... A0s, typename... A1s>
  RetT run(RetT (T::*md)(A0s...),
           A1s &&...args) requires ValidInvocationTypes<RetT, A0s...>;

 private:
  SplitMix64 split_mix64_;

  ShardedStatelessService(ShardedService<T> &&s);
  template <typename U, typename... As>
  friend ShardedStatelessService<U> make_sharded_stateless_service(
      As &&...args);
};

template <typename T, typename... As>
ShardedService<T> make_sharded_service(As &&...args);

template <typename T, typename... As>
ShardedStatelessService<T> make_sharded_stateless_service(As &&...args);

}  // namespace nu

#include "nu/impl/sharded_service.ipp"
