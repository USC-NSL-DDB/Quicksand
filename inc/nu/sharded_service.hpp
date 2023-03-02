#pragma once

#include "sharded_ds.hpp"

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

template <typename T, typename... As>
ShardedService<T> make_sharded_service(As &&...args);

}  // namespace nu

#include "nu/impl/sharded_service.ipp"
