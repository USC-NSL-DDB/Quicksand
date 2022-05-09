#include <sync.h>

#include "nu/runtime.hpp"

namespace nu {

extern void trampoline_in_proclet_env(void *arg);
extern void trampoline_in_runtime_env(void *arg);

inline Thread::Thread() : th_(nullptr), join_data_(nullptr) {}

inline Thread::~Thread() { BUG_ON(join_data_); }

inline Thread::Thread(Thread &&t) { *this = std::move(t); }

inline Thread &Thread::operator=(Thread &&t) {
  rt::Preempt p;
  rt::PreemptGuard guard(&p);
  th_ = t.th_;
  t.th_ = nullptr;
  join_data_ = t.join_data_;
  t.join_data_ = nullptr;
  thread_set_nu_thread(th_, this);
  return *this;
}

template <typename F> Thread::Thread(F &&f) {
  auto *proclet_header = Runtime::get_current_proclet_header();

  if (proclet_header) {
    create_in_proclet_env(f, proclet_header);
  } else {
    create_in_runtime_env(f);
  }
}

template <typename F>
void Thread::create_in_proclet_env(F &&f, ProcletHeader *header) {
  rt::Preempt p;
  rt::PreemptGuard g(&p);
  auto *proclet_stack = Runtime::stack_manager->get();
  assert(reinterpret_cast<uintptr_t>(proclet_stack) % kStackAlignment == 0);
  th_ = thread_nu_create_with_buf(
      this, proclet_stack, kStackSize, trampoline_in_proclet_env,
      reinterpret_cast<void **>(&join_data_), sizeof(*join_data_));
  BUG_ON(!th_);
  new (join_data_) join_data(std::forward<F>(f), header);
  thread_ready(th_);
}

template <typename F> void Thread::create_in_runtime_env(F &&f) {
  th_ = thread_create_with_buf(trampoline_in_runtime_env,
                               reinterpret_cast<void **>(&join_data_),
                               sizeof(*join_data_));
  BUG_ON(!th_);
  new (join_data_) join_data(std::forward<F>(f));
  thread_ready(th_);
}

inline bool Thread::joinable() { return join_data_; }
} // namespace nu
