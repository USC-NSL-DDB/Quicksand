#pragma once

#include <functional>
#include <memory>

#include "nu/utils/cond_var.hpp"
#include "nu/utils/spin_lock.hpp"

namespace nu {

struct ProcletHeader;

struct join_data {
  join_data(std::move_only_function<void()> f)
      : done(false), func(std::move(f)), header(nullptr) {}
  join_data(std::move_only_function<void()> f, ProcletHeader *hdr)
      : done(false), func(std::move(f)), header(hdr) {}

  bool done;
  SpinLock lock;
  CondVar cv;
  std::move_only_function<void()> func;
  ProcletHeader *header;
};

class Thread {
 public:
  Thread(std::move_only_function<void()> f, bool high_priority = false);
  Thread();
  ~Thread();
  Thread(const Thread &) = delete;
  Thread &operator=(const Thread &) = delete;
  Thread(Thread &&t);
  Thread &operator=(Thread &&t);
  bool joinable();
  void join();
  void detach();
  uint64_t get_id();
  static uint64_t get_current_id();

 private:
  uint64_t id_;
  join_data *join_data_;
  friend class Migrator;

  void create_in_proclet_env(std::move_only_function<void()> f,
                             ProcletHeader *header, bool head);
  void create_in_runtime_env(std::move_only_function<void()> f, bool head);
  static void trampoline_in_runtime_env(void *args);
  static void trampoline_in_proclet_env(void *args);
};

template <typename T, typename F>
void parallel_for(T begin_idx, T end_idx, F &&f);

}  // namespace nu

#include "nu/impl/thread.ipp"
