#pragma once

#include <memory>
#include <type_traits>

namespace nu {

template <typename T> class Promise;

template <typename T, typename Deleter = std::default_delete<Promise<T>>>
class Future {
public:
  Future();
  Future(const Future &) = delete;
  Future &operator=(const Future &) = delete;
  Future(Future &&);
  Future &operator=(Future &&);
  ~Future();
  operator bool() const;
  bool is_ready();
  T &get();

private:
  std::unique_ptr<Promise<T>, Deleter> promise_;
  template <typename U> friend class Promise;

  Future(Promise<T> *promise);
};

template <typename Deleter> class Future<void, Deleter> {
public:
  Future();
  Future(const Future &) = delete;
  Future &operator=(const Future &) = delete;
  Future(Future &&);
  Future &operator=(Future &&);
  ~Future();
  operator bool() const;
  bool is_ready();
  void get();

private:
  std::unique_ptr<Promise<void>, Deleter> promise_;
  template <typename U> friend class Promise;

  Future(Promise<void> *promise);
};

template <typename F, typename Allocator = std::allocator<
                          Promise<std::invoke_result_t<std::decay_t<F>>>>>
Future<std::invoke_result_t<std::decay_t<F>>> async(F &&f);

} // namespace nu

#include "nu/impl/future.ipp"
