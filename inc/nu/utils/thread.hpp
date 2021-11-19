#pragma once

#include <memory>

#include <sync.h>

#include "nu/heap_mgr.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/spinlock.hpp"

namespace nu {

struct join_data {
  template <typename F>
  join_data(F &&f) : done(false), func(std::forward<F>(f)) {}
  template <typename F>
  join_data(F &&f, OutermostMigrationDisabledGuard &&g)
      : done(false), func(std::forward<F>(f)), guard(std::move(g)) {}

  SpinLock lock;
  bool done;
  CondVar cv;
  folly::Function<void()> func;
  OutermostMigrationDisabledGuard guard;
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

private:
  thread_t *th_;
  join_data *join_data_;

  template <typename F> void create_in_obj_env(F &&f, HeapHeader *header);
  template <typename F> void create_in_runtime_env(F &&f);
  static void trampoline_in_runtime_env(void *args);
  static void trampoline_in_obj_env(void *args);
  static void __trampoline_in_obj_env(join_data *d, HeapHeader *heap_header);
};

} // namespace nu

#include "nu/impl/thread.ipp"
