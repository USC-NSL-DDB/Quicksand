#pragma once

#include "nu/cereal.hpp"
#include "nu/sharded_ds.hpp"
#include "nu/utils/splitmix64.hpp"

namespace nu {

template <typename T>
class ShardedStatelessService;

template <typename T>
class ShardedService
    : private ShardedDataStructure<GeneralContainer<T>, std::true_type> {
 public:
  using Key = T::Key;

  ShardedService() = default;
  template <typename RetT, typename... S0s, typename... S1s>
  RetT run(Key k, RetT (*fn)(T &, S0s...),
           S1s &&...states) requires ValidInvocationTypes<RetT, S0s...>;
  template <typename RetT, typename... A0s, typename... A1s>
  RetT run(Key k, RetT (T::*md)(A0s...),
           A1s &&...args) requires ValidInvocationTypes<RetT, A0s...>;
  template <class Archive>
  void serialize(Archive &ar);

 private:
  using Base = ShardedDataStructure<GeneralContainer<T>, std::true_type>;
  ShardedService(std::optional<typename Base::ShardingHint> sharding_hint);
  template <typename U, typename... As>
  friend ShardedService<U> make_sharded_service(As &&...args);
  template <typename U, typename... As>
  friend ShardedStatelessService<U> make_sharded_stateless_service(
      As &&...args);
};

template <typename T>
class ShardedStatelessService : private ShardedService<T> {
 public:
  ShardedStatelessService() = default;
  ShardedStatelessService(const ShardedStatelessService<T> &);
  ShardedStatelessService(ShardedStatelessService<T> &&) = default;
  ShardedStatelessService &operator=(const ShardedStatelessService<T> &);
  ShardedStatelessService &operator=(ShardedStatelessService<T> &&) = default;

  template <typename RetT, typename... S0s, typename... S1s>
  RetT run(RetT (*fn)(T &, S0s...),
           S1s &&...states) requires ValidInvocationTypes<RetT, S0s...>;
  template <typename RetT, typename... A0s, typename... A1s>
  RetT run(RetT (T::*md)(A0s...),
           A1s &&...args) requires ValidInvocationTypes<RetT, A0s...>;
  template <class Archive>
  void serialize(Archive &ar);

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
