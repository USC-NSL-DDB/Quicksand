#pragma once

extern "C" {
#include <base/compiler.h>
}
#include "sync.h"

#include "utils/promise.hpp"

namespace nu {

template <typename T, typename Deleter> Future<T, Deleter>::Future() {}

template <typename T, typename Deleter>
Future<T, Deleter>::Future(Promise<T> *promise)
    : promise_(promise, Deleter()) {}

template <typename T, typename Deleter>
Future<T, Deleter>::Future(Future<T, Deleter> &&o)
    : promise_(std::move(o.promise_)) {}

template <typename T, typename Deleter>
Future<T, Deleter> &Future<T, Deleter>::operator=(Future<T, Deleter> &&o) {
  promise_ = std::move(o.promise_);
  return *this;
}

template <typename Deleter> Future<void, Deleter>::Future() {}

template <typename Deleter>
Future<void, Deleter>::Future(Promise<void> *promise)
    : promise_(promise, Deleter()) {}

template <typename Deleter>
Future<void, Deleter>::Future(Future<void, Deleter> &&o)
    : promise_(std::move(o.promise_)) {}

template <typename Deleter>
Future<void, Deleter> &Future<void, Deleter>::
operator=(Future<void, Deleter> &&o) {
  promise_ = std::move(o.promise_);
  return *this;
}

template <typename T, typename Deleter> Future<T, Deleter>::~Future() {
  if (promise_) {
    get();
  }
}

template <typename Deleter> Future<void, Deleter>::~Future() {
  if (promise_) {
    get();
  }
}

template <typename T, typename Deleter>
Future<T, Deleter>::operator bool() const {
  return promise_.get();
}

template <typename Deleter> Future<void, Deleter>::operator bool() const {
  return promise_.get();
}

template <typename T, typename Deleter> bool Future<T, Deleter>::is_ready() {
  return ACCESS_ONCE(promise_->ready_);
}

template <typename Deleter> bool Future<void, Deleter>::is_ready() {
  return ACCESS_ONCE(promise_->ready_);
}

template <typename T, typename Deleter> T &Future<T, Deleter>::get() {
  if (is_ready()) {
    return promise_->t_;
  }

  rt::ScopedLock<rt::Mutex> lock(&promise_->mutex_);
  while (!is_ready()) {
    promise_->cv_.Wait(&promise_->mutex_);
  }
  return promise_->t_;
}

template <typename Deleter> void Future<void, Deleter>::get() {
  if (is_ready()) {
    return;
  }

  rt::ScopedLock<rt::Mutex> lock(&promise_->mutex_);
  while (!is_ready()) {
    promise_->cv_.Wait(&promise_->mutex_);
  }
}
} // namespace nu
