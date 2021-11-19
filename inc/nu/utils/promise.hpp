#pragma once

#include <functional>
#include <memory>

#include "nu/utils/thread.hpp"

namespace nu {

template <typename T, typename Deleter> class Future;

template <typename T> class Promise {
public:
  Promise(const Promise &) = delete;
  Promise &operator=(const Promise &) = delete;
  ~Promise();
  template <typename Deleter = std::default_delete<Promise>>
  Future<T, Deleter> get_future();
  template <typename F, typename Allocator = std::allocator<Promise>>
  static Promise *create(F &&f);

private:
  bool futurized_;
  T t_;
  Thread th_;
  template <typename U, typename Deleter> friend class Future;

  Promise();
  T *data();
};

template <> class Promise<void> {
public:
  Promise(const Promise &) = delete;
  Promise &operator=(const Promise &) = delete;
  ~Promise();
  template <typename Deleter = std::default_delete<Promise>>
  Future<void, Deleter> get_future();
  template <typename F, typename Allocator = std::allocator<Promise>>
  static Promise *create(F &&f);

private:
  bool futurized_;
  Thread th_;
  template <typename U, typename Deleter> friend class Future;

  Promise();
};
} // namespace nu

#include "nu/impl/promise.ipp"
