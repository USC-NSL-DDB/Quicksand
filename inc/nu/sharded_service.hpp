#pragma once

#include <cstdint>

#include "nu/cereal.hpp"
#include "nu/sharded_ds.hpp"
#include "nu/utils/splitmix64.hpp"

namespace nu {

struct ServicePassThrough {};

template <typename T, BoolIntegral Stateful>
class Service;

template <typename T>
using StatefulService = Service<T, std::true_type>;

template <typename T>
using StatelessService = Service<T, std::false_type>;

template <typename T>
class ShardedService
    : private ShardedDataStructure<GeneralContainer<StatefulService<T>>,
                                   std::true_type> {
 public:
  using Key = T::Key;

  ShardedService() = default;
  ShardedService(const ShardedService &) = default;
  ShardedService(ShardedService &&) = default;
  ShardedService &operator=(const ShardedService &) = default;
  ShardedService &operator=(ShardedService &&) = default;
  template <typename RetT, typename... S0s, typename... S1s>
  RetT run(Key k, RetT (*fn)(T &, S0s...), S1s &&...states)
    requires ValidInvocationTypes<RetT, S0s...>;
  template <typename RetT, typename... A0s, typename... A1s>
  RetT run(Key k, RetT (T::*md)(A0s...), A1s &&...args)
    requires ValidInvocationTypes<RetT, A0s...>;
  template <class Archive>
  void serialize(Archive &ar);

 private:
  using Base = ShardedDataStructure<GeneralContainer<StatefulService<T>>,
                                    std::true_type>;
  template <typename... As>
  ShardedService(std::optional<typename Base::ShardingHint> sharding_hint,
                 As &&...args);
  template <typename U, typename... As>
  friend ShardedService<U> make_sharded_service(As &&...args);
};

template <typename T>
class ShardedStatelessService
    : private ShardedDataStructure<GeneralContainer<StatelessService<T>>,
                                   std::true_type> {
 public:
  ShardedStatelessService() = default;
  ShardedStatelessService(const ShardedStatelessService &);
  ShardedStatelessService(ShardedStatelessService &&) = default;
  ShardedStatelessService &operator=(const ShardedStatelessService &);
  ShardedStatelessService &operator=(ShardedStatelessService &&) = default;
  template <typename RetT, typename... S0s, typename... S1s>
  RetT run(RetT (*fn)(T &, S0s...), S1s &&...states)
    requires ValidInvocationTypes<RetT, S0s...>;
  template <typename RetT, typename... A0s, typename... A1s>
  RetT run(RetT (T::*md)(A0s...), A1s &&...args)
    requires ValidInvocationTypes<RetT, A0s...>;
  template <class Archive>
  void serialize(Archive &ar);

 private:
  using Base = ShardedDataStructure<GeneralContainer<StatelessService<T>>,
                                    std::true_type>;
  SplitMix64 split_mix64_;

  template <typename... As>
  ShardedStatelessService(
      std::optional<typename Base::ShardingHint> sharding_hint, As &&...args);
  template <typename U, typename... As>
  friend ShardedStatelessService<U> make_sharded_stateless_service(
      As &&...args);
};

template <typename T, typename... As>
ShardedService<T> make_sharded_service(As &&...args);

template <typename T, typename... As>
ShardedStatelessService<T> make_sharded_stateless_service(As &&...args);

// Boilerplate to adapt to the sharded data structure API.
template <typename T, BoolIntegral Sf>
class Service {
 public:
  constexpr static bool Stateful = Sf::value;
  using Key = decltype([] {
    if constexpr (Stateful) {
      return typename T::Key();
    } else {
      return uint64_t();
    }
  }());

  Service() = default;
  Service(const Service &) = default;
  Service(Service &&) = default;
  Service &operator=(const Service &) = default;
  Service &operator=(Service &&) = default;
  Service(const T &t) : t_(t) {}
  Service(T &&t) : t_(std::move(t)) {}
  template <typename... As>
  Service(ServicePassThrough, As &&...args) : t_(std::forward<As>(args)...) {}
  bool empty() const { return false; }
  std::size_t size() const { return 1; }
  void split(Key *mid_k, Service *latter_half) {
    if constexpr (Stateful) {
      t_.split(mid_k, &latter_half->t_);
    }
  }
  template <typename RetT, typename... S0s, typename... S1s>
  RetT compute(RetT (*fn)(T &t, S0s...), S1s &&...states) {
    return fn(t_, std::forward<S1s>(states)...);
  }
  template <typename RetT, typename... A0s, typename... A1s>
  RetT compute(RetT (T::*md)(A0s...), A1s &&...args) {
    return (t_.*md)(std::forward<A1s>(args)...);
  }
  template <class Archive>
  void serialize(Archive &ar) {
    ar(t_);
  }

 private:
  T t_;
};

}  // namespace nu

#include "nu/impl/sharded_service.ipp"
