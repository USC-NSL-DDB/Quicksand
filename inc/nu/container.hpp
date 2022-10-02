#pragma once

#include <concepts>
#include <optional>
#include <utility>
#include <vector>

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
concept EmplaceBackAble = requires(T t) {
  { t.emplace_back(std::declval<typename T::Val>()) }
  ->std::same_as<void>;
};

template <class T>
concept Findable = requires(T t) {
  { t.find(std::declval<typename T::Key>()) }
  ->std::same_as<typename T::ConstIterator>;
};

template <class T>
concept ConstIterable = requires(T t) {
  { t.cbegin() }
  ->std::same_as<typename T::ConstIterator>;
  { t.cend() }
  ->std::same_as<typename T::ConstIterator>;
};

template <class T>
concept ConstReverseIterable = requires(T t) {
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
  using Val = Impl::Val;
  using Pair = std::pair<Key, Val>;
  using ConstIterator = decltype([] {
    if constexpr (ConstIterable<Impl>) {
      return typename Impl::ConstIterator();
    } else {
      return new ErasedType();
    }
  }());
  using ConstReverseIterator = decltype([] {
    if constexpr (ConstReverseIterable<Impl>) {
      return typename Impl::ConstReverseIterator();
    } else {
      return new ErasedType();
    }
  }());
  using IterVal = DeepDecay_t<decltype(*std::declval<ConstIterator>())>;
  using ContainerType =
      std::conditional_t<Synchronized::value, GeneralLockedContainer<Impl>,
                         GeneralContainer<Impl>>;

  GeneralContainerBase() : impl_() {}
  GeneralContainerBase(std::optional<Key> l_key) : impl_(l_key) {}
  GeneralContainerBase(std::optional<Key> l_key, std::size_t capacity)
      : impl_(l_key, capacity) {}
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
  std::size_t size() {
    return synchronized<std::size_t>([&] { return impl_.size(); });
  }
  std::size_t capacity() {
    return synchronized<std::size_t>([&] { return impl_.capacity(); });
  }
  bool empty() {
    return synchronized<bool>([&] { return impl_.empty(); });
  };
  void clear() {
    return synchronized<void>([&] { return impl_.clear(); });
  };
  void emplace(Key k, Val v) {
    synchronized<void>([&] { impl_.emplace(std::move(k), std::move(v)); });
  }
  void emplace_back(Val v) requires EmplaceBackAble<Impl> {
    synchronized<void>([&] { impl_.emplace_back(std::move(v)); });
  }
  void emplace_back_batch(std::vector<Val> v) requires EmplaceBackAble<Impl> {
    synchronized<void>([&] { impl_.emplace_back_batch(std::move(v)); });
  }
  ConstIterator find(Key k) requires Findable<Impl> {
    return synchronized<ConstIterator>(
        [&] { return impl_.find(std::move(k)); });
  }
  std::pair<Key, ContainerType> split() {
    return synchronized<std::pair<Key, ContainerType>>([&] {
      auto [k, impl] = impl_.split();
      ContainerType c;
      c.impl_ = std::move(impl);
      return std::make_pair(std::move(k), std::move(c));
    });
  }
  void merge(ContainerType c) {
    synchronized<void>([&] { impl_.merge(c.impl_); });
  }
  template <typename... S0s, typename... S1s>
  void for_all(void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states) {
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
  template <class Archive>
  void save(Archive &ar) const {
    impl_.save(ar);
  }
  template <class Archive>
  void load(Archive &ar) {
    impl_.load(ar);
  }
  void emplace_batch(std::vector<std::pair<Key, Val>> reqs);

 private:
  Impl impl_;
  Mutex mutex_;

  template <typename RetT, typename F>
  RetT synchronized(F &&f);
};

}  // namespace nu

#include "nu/impl/container.ipp"
