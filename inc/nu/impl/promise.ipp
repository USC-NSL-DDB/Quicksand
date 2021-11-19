#pragma once

extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
}
#include <sync.h>

#include "nu/utils/future.hpp"

namespace nu {

template <typename T> Promise<T>::Promise() : futurized_(false) {}

inline Promise<void>::Promise() : futurized_(false) {}

template <typename T> Promise<T>::~Promise() {
  if (th_.joinable()) {
    th_.join();
  }
}

inline Promise<void>::~Promise() {
  if (th_.joinable()) {
    th_.join();
  }
}

template <typename T>
template <typename Deleter>
Future<T, Deleter> Promise<T>::get_future() {
  BUG_ON(futurized_);
  futurized_ = true;
  return Future<T, Deleter>(this);
}

template <typename Deleter> Future<void, Deleter> Promise<void>::get_future() {
  BUG_ON(futurized_);
  futurized_ = true;
  return Future<void, Deleter>(this);
}

template <typename T> T *Promise<T>::data() { return &t_; }

template <typename T>
template <typename F, typename Allocator>
Promise<T> *Promise<T>::create(F &&f) {
  Allocator allocator;
  auto *promise = allocator.allocate(1);
  new (promise) Promise<T>();
  promise->th_ = Thread(
      [promise, f = std::forward<F>(f)]() mutable { *promise->data() = f(); });
  return promise;
}

template <typename F, typename Allocator>
Promise<void> *Promise<void>::create(F &&f) {
  Allocator allocator;
  auto *promise = allocator.allocate(1);
  new (promise) Promise<void>();
  promise->th_ = Thread([promise, f = std::forward<F>(f)]() mutable { f(); });
  return promise;
}

} // namespace nu
