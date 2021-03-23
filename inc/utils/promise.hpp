#pragma once

#include <functional>
#include <memory>

#include "sync.h"

namespace nu {

template <typename T, typename Deleter> class Future;

template <typename T> class Promise {
public:
  Promise(const Promise &) = delete;
  Promise &operator=(const Promise &) = delete;
  ~Promise();
  template <typename Deleter = std::default_delete<Promise>>
  Future<T, Deleter> get_future();
  template <typename Allocator = std::allocator<Promise>>
  static Promise *create(const std::function<T()> &func);
  template <typename Allocator = std::allocator<Promise>>
  static Promise *create(std::function<T()> &&func);

private:
  bool futurized_;
  bool ready_;
  rt::Mutex mutex_;
  rt::CondVar cv_;
  T t_;
  template <typename U, typename Deleter> friend class Future;

  Promise();
  void set_ready();
  T *data();
};

template <> class Promise<void> {
public:
  Promise(const Promise &) = delete;
  Promise &operator=(const Promise &) = delete;
  ~Promise();
  template <typename Deleter = std::default_delete<Promise>>
  Future<void, Deleter> get_future();
  template <typename Allocator = std::allocator<Promise>>  
  static Promise *create(const std::function<void()> &func);
  template <typename Allocator = std::allocator<Promise>>
  static Promise *create(std::function<void()> &&func);

private:
  bool futurized_;
  bool ready_;
  rt::Mutex mutex_;
  rt::CondVar cv_;
  template <typename U, typename Deleter> friend class Future;

  Promise();
  void set_ready();
};
} // namespace nu

#include "impl/promise.ipp"
