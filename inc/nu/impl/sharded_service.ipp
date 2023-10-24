#include <deque>
#include <limits>

extern "C" {
#include <asm/ops.h>
}

#include "nu/resource_reporter.hpp"

namespace nu {

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT ShardedService<T>::run(Key k, RetT (*fn)(T &, S0s...), S1s &&...states)
  requires ValidInvocationTypes<RetT, S0s...>
{
  using fn_states_checker [[maybe_unused]] =
      decltype(fn(std::declval<T &>(), std::move(states)...));

  auto fn_addr = reinterpret_cast<uintptr_t>(fn);
  auto *proxy_fn =
      +[](StatefulService<T> &service, uintptr_t fn_addr, S0s... states) {
        return service.compute(reinterpret_cast<decltype(fn)>(fn_addr),
                               std::move(states)...);
      };
  return this->compute_on(k, proxy_fn, fn_addr, std::forward<S1s>(states)...);
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT ShardedService<T>::run(Key k, RetT (T::*md)(A0s...), A1s &&...args)
  requires ValidInvocationTypes<RetT, A0s...>
{
  using md_args_checker [[maybe_unused]] =
      decltype((std::declval<T>().*(md))(std::forward<A1s>(args)...));

  MethodPtr<decltype(md)> method_ptr;
  method_ptr.ptr = md;
  auto *proxy_fn = +[](StatefulService<T> &service,
                       decltype(method_ptr) method_ptr, A0s... args) {
    return service.compute(method_ptr.ptr, std::move(args)...);
  };
  return this->compute_on(k, proxy_fn, method_ptr, std::forward<A1s>(args)...);
}

template <typename T>
template <class Archive>
void ShardedService<T>::serialize(Archive &ar) {
  ar(*static_cast<Base *>(this));
}

template <typename T>
ShardedStatelessService<T>::ShardedStatelessService(
    const ShardedStatelessService<T> &o)
    : Base(o), split_mix64_() {}

template <typename T>
ShardedStatelessService<T> &ShardedStatelessService<T>::operator=(
    const ShardedStatelessService<T> &o) {
  Base::operator=(o);
  split_mix64_ = SplitMix64();
  return *this;
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT ShardedStatelessService<T>::run(RetT (*fn)(T &, S0s...), S1s &&...states)
  requires ValidInvocationTypes<RetT, S0s...>
{
  using fn_states_checker [[maybe_unused]] =
      decltype(fn(std::declval<T &>(), std::move(states)...));

  auto fn_addr = reinterpret_cast<uintptr_t>(fn);
  auto *proxy_fn =
      +[](StatelessService<T> &service, uintptr_t fn_addr, S0s... states) {
        return service.compute(reinterpret_cast<decltype(fn)>(fn_addr),
                               std::move(states)...);
      };
  return this->compute_on(split_mix64_.next(), proxy_fn, fn_addr,
                          std::forward<S1s>(states)...);
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT ShardedStatelessService<T>::run(RetT (T::*md)(A0s...), A1s &&...args)
  requires ValidInvocationTypes<RetT, A0s...>
{
  using md_args_checker [[maybe_unused]] =
      decltype((std::declval<T>().*(md))(std::forward<A1s>(args)...));

  MethodPtr<decltype(md)> method_ptr;
  method_ptr.ptr = md;
  auto *proxy_fn = +[](StatelessService<T> &service,
                       decltype(method_ptr) method_ptr, A0s... args) {
    return service.compute(method_ptr.ptr, std::move(args)...);
  };
  return this->compute_on(split_mix64_.next(), proxy_fn, method_ptr,
                          std::forward<A1s>(args)...);
}

template <typename T>
template <class Archive>
void ShardedStatelessService<T>::serialize(Archive &ar) {
  ar(*static_cast<Base *>(this));
}

template <typename T>
template <typename... As>
inline ShardedService<T>::ShardedService(
    std::optional<typename Base::ShardingHint> sharding_hint, As &&...args)
    : Base(sharding_hint,
           /* pinned_ip = */ std::nullopt, ServicePassThrough(),
           std::forward<As>(args)...) {}

template <typename T, typename... As>
inline ShardedService<T> make_sharded_service(As &&...args) {
  return ShardedService<T>(/* sharding_hint = */ std::nullopt,
                           std::forward<As>(args)...);
}

template <typename T>
template <typename... As>
inline ShardedStatelessService<T>::ShardedStatelessService(
    std::optional<typename Base::ShardingHint> sharding_hint, As &&...args)
    : Base(sharding_hint,
           /* pinned_ip = */ std::nullopt, ServicePassThrough(),
           std::forward<As>(args)...) {}

template <typename T, typename... As>
inline ShardedStatelessService<T> make_sharded_stateless_service(As &&...args) {
  std::vector<std::pair<NodeIP, Resource>> global_free_resources;
  {
    Caladan::PreemptGuard g;
    global_free_resources =
        get_runtime()->resource_reporter()->get_global_free_resources();
  }
  auto ratio = 1.0f / global_free_resources.size();
  std::deque<uint64_t> rkeys;
  for (std::size_t i = 0; i < global_free_resources.size(); i++) {
    auto last_rkey = rkeys.empty() ? 0 : rkeys.back();
    auto new_rkey = last_rkey + ratio * std::numeric_limits<uint64_t>::max();
    rkeys.push_back(new_rkey);
  }
  using Base = ShardedStatelessService<T>::Base;
  typename Base::ShardingHint h;
  h.num = global_free_resources.size() *
          (Base::kLowLatencyMaxShardBytes / sizeof(uint64_t));
  h.estimated_min_key = 0;
  h.key_inc_fn = std::function(
      [rkeys = std::move(rkeys)](uint64_t &k, uint64_t _) mutable {
        k = rkeys.front();
        rkeys.pop_front();
      });
  return ShardedStatelessService<T>(h, std::forward<As>(args)...);
}

}  // namespace nu
