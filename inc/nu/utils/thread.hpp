#pragma once

#include <folly/Function.h>
#include <memory>

#include <sync.h>

#include "nu/proclet_mgr.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/spinlock.hpp"

namespace nu {

struct join_data {
  template <typename F>
  join_data(F &&f) : done(false), func(std::move(f)), header(nullptr) {}
  template <typename F>
  join_data(F &&f, ProcletHeader *hdr)
      : done(false), func(std::move(f)), header(hdr) {}

  bool done;
  SpinLock lock;
  CondVar cv;
  folly::Function<void()> func;
  ProcletHeader *header;
};

class Thread {
public:
  using id = thread_id_t;

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
  // Warning: in the current implementation, the ID may change after migration.
  id get_id() { return get_thread_id(th_); }
  static id get_current_id() { return get_current_thread_id(); }

private:
  thread_t *th_;
  join_data *join_data_;
  friend class Migrator;

  template <typename F>
  void create_in_proclet_env(F &&f, ProcletHeader *header);
  template <typename F> void create_in_runtime_env(F &&f);
  static void trampoline_in_runtime_env(void *args);
  static void trampoline_in_proclet_env(void *args);
};

} // namespace nu

#include "nu/impl/thread.ipp"
