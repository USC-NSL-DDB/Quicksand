#pragma once

#include <concepts>
#include <cstdint>
#include <functional>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "nu/commons.hpp"
#include "nu/type_traits.hpp"
#include "nu/utils/mutex.hpp"

namespace nu {

template <class Impl, BoolIntegral Synchronized>
class GeneralContainerBase;

template <class T>
concept GeneralContainerBased = requires {
  requires is_base_of_template_v<T, GeneralContainerBase>;
};

template <class T>
concept PushFrontAble = requires(T t) {
  { t.push_front(std::declval<typename T::Val>()) }
  ->std::same_as<std::size_t>;
};

template <class T>
concept PushBackAble = requires(T t) {
  { t.push_back(std::declval<typename T::Val>()) }
  ->std::same_as<std::size_t>;
};

template <class T>
concept InsertAbleByPair = requires(T t) {
  { t.insert(std::declval<typename T::Key>(), std::declval<typename T::Val>()) }
  ->std::same_as<std::size_t>;
};

template <class T>
concept InsertAbleByKey = requires(T t) {
  {
    t.insert(std::declval<typename T::Key>())
  }
  ->std::same_as<std::size_t>;
};

template <class T>
concept InsertAble = requires(T t) {
  requires(InsertAbleByPair<T> || InsertAbleByKey<T>);
};

template <class T>
concept HasVal = requires {
  requires !std::is_same_v<typename T::Val, ErasedType>;
};

template <class T>
concept HasFront = requires(T t) {
  { t.front() }
  ->std::same_as<typename T::Val>;
};

template <class T>
concept PopFrontAble = requires(T t) {
  { t.pop_front() }
  ->std::same_as<std::optional<typename T::Val>>;
};

template <class T>
concept TryPopFrontAble = requires(T t) {
  { t.try_pop_front(std::declval<std::size_t>()) }
  ->std::same_as<std::vector<typename T::Val>>;
};

template <class T>
concept HasBack = requires(T t) {
  { t.back() }
  ->std::same_as<typename T::Val>;
};

template <class T>
concept PopBackAble = requires(T t) {
  { t.pop_back() }
  ->std::same_as<std::optional<typename T::Val>>;
};

template <class T>
concept TryPopBackAble = requires(T t) {
  { t.try_pop_back(std::declval<std::size_t>()) }
  ->std::same_as<std::vector<typename T::Val>>;
};

template <class T>
concept ClearAble = requires(T t) {
  { t.clear() }
  ->std::same_as<void>;
};

template <class T>
concept FindAble = requires(T t) {
  { t.find(std::declval<typename T::Key>()) }
  ->std::same_as<typename T::ConstIterator>;
};

template <class T>
concept FindAbleByOrder = requires(T t) {
  { t.find_by_order(std::declval<std::size_t>()) }
  ->std::same_as<typename T::ConstIterator>;
};

template <class T>
concept HasCapacity = requires(T t) {
  { t.capacity() }
  ->std::same_as<std::size_t>;
};

template <class T>
concept Reservable = requires(T t) {
  { t.reserve(std::declval<std::size_t>()) }
  ->std::same_as<void>;
};

template <class T>
concept RebaseAble = requires(T t) {
  { t.rebase(std::declval<typename T::Key>()) }
  ->std::same_as<typename T::Key>;
};

template <class T>
concept UInt64Convertable = requires(T t) {
  requires sizeof(T) == sizeof(uint64_t);
  requires std::is_trivially_copy_assignable_v<T>;
};

template <class T>
concept ConstIterable = requires(T t) {
  requires UInt64Convertable<typename T::ConstIterator>;
  { t.cbegin() }
  ->std::same_as<typename T::ConstIterator>;
  { t.cend() }
  ->std::same_as<typename T::ConstIterator>;
};

template <class T>
concept ConstReverseIterable = requires(T t) {
  requires UInt64Convertable<typename T::ConstReverseIterator>;
  { t.crbegin() }
  ->std::same_as<typename T::ConstReverseIterator>;
  { t.crend() }
  ->std::same_as<typename T::ConstReverseIterator>;
};

template <class Impl>
using GeneralContainer = GeneralContainerBase<Impl, std::false_type>;

template <class Impl>
using GeneralLockedContainer = GeneralContainerBase<Impl, std::true_type>;

template <class Impl, BoolIntegral Synchronized>
class GeneralContainerBase {
 public:
  using Key = Impl::Key;
  using Val = decltype([] {
    if constexpr (HasVal<Impl>) {
      return typename Impl::Val();
    } else {
      return ErasedType();
    }
  }());
  using DataEntry = std::conditional_t<HasVal<Impl>, std::pair<Key, Val>, Key>;
  using Implementation = Impl;
  using ConstIterator = decltype([] {
    if constexpr (ConstIterable<Impl>) {
      return typename Impl::ConstIterator();
    } else {
      return new ErasedType();
    }
  }());
  constexpr static bool kContiguousIterator = [] {
    if constexpr (ConstIterable<Impl>) {
      return Impl::ConstIterator::kContiguous;
    } else {
      return false;
    }
  }();
  using ConstReverseIterator = decltype([] {
    if constexpr (ConstReverseIterable<Impl>) {
      return typename Impl::ConstReverseIterator();
    } else {
      return new ErasedType();
    }
  }());
  constexpr static bool kContiguousReverseIterator = [] {
    if constexpr (ConstReverseIterable<Impl>) {
      return Impl::ConstReverseIterator::kContiguous;
    } else {
      return false;
    }
  }();
  using IterVal = DeepDecay_t<decltype(*std::declval<ConstIterator>())>;
  using ContainerType =
      std::conditional_t<Synchronized::value, GeneralLockedContainer<Impl>,
                         GeneralContainer<Impl>>;

  GeneralContainerBase() : impl_() {}
  GeneralContainerBase(const GeneralContainerBase &c) : impl_(c.impl_) {}
  GeneralContainerBase &operator=(const GeneralContainerBase &c) {
    impl_ = c.impl_;
    return *this;
  }
  GeneralContainerBase(GeneralContainerBase &&c) noexcept
      : impl_(std::move(c.impl_)) {}
  GeneralContainerBase &operator=(GeneralContainerBase &&c) noexcept {
    impl_ = std::move(c.impl_);
    return *this;
  }
  std::size_t size() const {
    return synchronized<std::size_t>([&] { return impl_.size(); });
  }
  std::size_t capacity() const requires HasCapacity<Impl> {
    return synchronized<std::size_t>([&] { return impl_.capacity(); });
  }
  void reserve(std::size_t size) requires Reservable<Impl> {
    synchronized<void>([&] { impl_.reserve(size); });
  }
  bool empty() const {
    return synchronized<bool>([&] { return impl_.empty(); });
  };
  void clear() requires ClearAble<Impl> {
    return synchronized<void>([&] { return impl_.clear(); });
  };
  std::size_t insert(Key k, Val v) requires(HasVal<Impl> && InsertAble<Impl>) {
    return synchronized<std::size_t>(
        [&] { return impl_.insert(std::move(k), std::move(v)); });
  }
  std::size_t insert(Key k) requires(!HasVal<Impl> && InsertAble<Impl>) {
    return synchronized<std::size_t>(
        [&] { return impl_.insert(std::move(k)); });
  }
  std::size_t push_front(Val v) requires PushFrontAble<Impl> {
    return synchronized<std::size_t>([&] { return impl_.push_front(v); });
  }
  std::size_t push_back(Val v) requires PushBackAble<Impl> {
    return synchronized<std::size_t>(
        [&] { return impl_.push_back(std::move(v)); });
  }
  std::size_t push_back_batch(
      std::vector<Val> v) requires PushBackAble<Impl> {
    return synchronized<std::size_t>(
        [&] { return impl_.push_back_batch(std::move(v)); });
  }
  ConstIterator find(Key k) const requires FindAble<Impl> {
    return synchronized<ConstIterator>(
        [&] { return impl_.find(std::move(k)); });
  }
  ConstIterator find_by_order(
      std::size_t order) requires FindAbleByOrder<Impl> {
    return synchronized<ConstIterator>(
        [&] { return impl_.find_by_order(order); });
  }
  Val front() const requires HasFront<Impl> {
    return synchronized<Val>([&] { return impl_.front(); });
  }
  std::optional<Val> pop_front() requires PopFrontAble<Impl> {
    return synchronized<std::optional<Val>>([&] { return impl_.pop_front(); });
  }
  std::vector<Val> try_pop_front(
      std::size_t num) requires TryPopFrontAble<Impl> {
    return synchronized<std::vector<Val>>(
        [&] { return impl_.try_pop_front(num); });
  }
  Val back() const requires HasBack<Impl> {
    return synchronized<Val>([&] { return impl_.back(); });
  }
  std::optional<Val> pop_back() requires PopBackAble<Impl> {
    return synchronized<std::optional<Val>>([&] { return impl_.pop_back(); });
  }
  std::vector<Val> try_pop_back(std::size_t num) requires TryPopBackAble<Impl> {
    return synchronized<std::vector<Val>>(
        [&] { return impl_.try_pop_back(num); });
  }
  void split(Key *mid_k, ContainerType *latter_half) {
    return synchronized<void>([&] { impl_.split(mid_k, &latter_half->impl_); });
  }
  void merge(ContainerType c) {
    synchronized<void>([&] { impl_.merge(std::move(c.impl_)); });
  }
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...),
               S1s &&... states) requires HasVal<Impl> {
    synchronized<void>(
        [&] { impl_.for_all(fn, std::forward<S1s>(states)...); });
  }
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, S0s...),
               S1s &&... states) requires(!HasVal<Impl>) {
    synchronized<void>(
        [&] { impl_.for_all(fn, std::forward<S1s>(states)...); });
  }
  Impl &unwrap() { return impl_; }
  ConstIterator cbegin() const requires ConstIterable<Impl> {
    return impl_.cbegin();
  }
  ConstIterator cend() const requires ConstIterable<Impl> {
    return impl_.cend();
  }
  ConstReverseIterator crbegin() const requires ConstReverseIterable<Impl> {
    return impl_.crbegin();
  }
  ConstReverseIterator crend() const requires ConstReverseIterable<Impl> {
    return impl_.crend();
  }
  Key rebase(Key new_l_key) requires RebaseAble<Impl> {
    return impl_.rebase(new_l_key);
  }
  template <typename... S0s, typename... S1s>
  void pass_through(void (*fn)(Impl &, S0s...), S1s &&... states) {
    synchronized<void>([&] { fn(impl_, std::move(states)...); });
  }
  template <class Archive>
  void save(Archive &ar) const {
    impl_.save(ar);
  }
  template <class Archive>
  void load(Archive &ar) {
    impl_.load(ar);
  }

 private:
  Impl impl_;
  Mutex mutex_;
  template <GeneralContainerBased Container>
  friend class GeneralShard;

  template <typename RetT, typename F>
  RetT synchronized(F &&f) const;
  std::size_t insert_batch(
      std::vector<DataEntry> reqs) requires InsertAble<Impl>;
};

}  // namespace nu

#include "nu/impl/container.ipp"
