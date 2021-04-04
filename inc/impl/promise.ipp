#pragma once

extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
}
#include "sync.h"
#include "thread.h"

#include "utils/future.hpp"

namespace nu {

template <typename T>
Promise<T>::Promise() : futurized_(false), ready_(false) {}

inline Promise<void>::Promise() : futurized_(false), ready_(false) {}

template <typename T> Promise<T>::~Promise() {}

inline Promise<void>::~Promise() {}

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

template <typename T> void Promise<T>::set_ready() {
  rt::ScopedLock<rt::Mutex> l(&mutex_);
  ready_ = true;
  cv_.SignalAll();
}

inline void Promise<void>::set_ready() {
  rt::ScopedLock<rt::Mutex> l(&mutex_);
  ready_ = true;
  cv_.SignalAll();
}

template <typename T> T *Promise<T>::data() { return &t_; }

template <typename T>
template <typename Allocator>
Promise<T> *Promise<T>::create(const std::function<T()> &func) {
  Allocator allocator;
  auto *promise = allocator.allocate(1);
  new (promise) Promise<T>();
  rt::Thread([promise, func = std::move(func)] {
    *promise->data() = func();
    promise->set_ready();
  })
      .Detach();
  return promise;
}

template <typename T>
template <typename Allocator>
Promise<T> *Promise<T>::create(std::function<T()> &&func) {
  Allocator allocator;
  auto *promise = allocator.allocate(1);
  new (promise) Promise<T>();
  rt::Thread([promise, func = std::move(func)] {
    *promise->data() = func();
    promise->set_ready();
  })
      .Detach();
  return promise;
}

template <typename Allocator>
Promise<void> *Promise<void>::create(const std::function<void()> &func) {
  Allocator allocator;
  auto *promise = allocator.allocate(1);
  new (promise) Promise<void>();
  rt::Thread([promise, func = std::move(func)] {
    func();
    promise->set_ready();
  })
      .Detach();
  return promise;
}

template <typename Allocator>
Promise<void> *Promise<void>::create(std::function<void()> &&func) {
  Allocator allocator;
  auto *promise = allocator.allocate(1);
  new (promise) Promise<void>();
  rt::Thread([promise, func = std::move(func)] {
    func();
    promise->set_ready();
  })
      .Detach();
  return promise;
}
} // namespace nu
