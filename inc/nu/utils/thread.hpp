#pragma once

#include <folly/Function.h>
#include <memory>

#include <sync.h>

#include "nu/heap_mgr.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/spinlock.hpp"

namespace nu {

struct join_data {
  template <typename F>
  join_data(F &&f) : done(false), func(std::move(f)), header(nullptr) {}
  template <typename F>
  join_data(F &&f, HeapHeader *hdr)
      : done(false), func(std::move(f)), header(hdr) {}

  SpinLock lock;
  bool done;
  CondVar cv;
  folly::Function<void()> func;
  HeapHeader *header;
};

class Thread {
public:
  template <typename F> Thread(F &&f);
  Thread();
  ~Thread();
  Thread(const Thread &) = delete;
  Thread &operator=(const Thread &) = delete;
  Thread(Thread &&t);
  Thread &operator=(Thread &&t);

  bool joinable();
  void join();
  void detach();

private:
  thread_t *th_;
  join_data *join_data_;
  friend class Migrator;

  template <typename F> void create_in_obj_env(F &&f, HeapHeader *header);
  template <typename F> void create_in_runtime_env(F &&f);
  static void trampoline_in_runtime_env(void *args);
  static void trampoline_in_obj_env(void *args);
};

} // namespace nu

#include "nu/impl/thread.ipp"
