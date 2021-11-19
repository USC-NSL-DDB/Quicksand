#include "nu/heap_mgr.hpp"
#include "nu/runtime.hpp"

namespace nu {

extern void trampoline_in_obj_env(void *arg);
extern void trampoline_in_runtime_env(void *arg);

inline Thread::Thread() : th_(nullptr), join_data_(nullptr) {}

inline Thread::~Thread() { BUG_ON(join_data_); }

inline Thread::Thread(Thread &&t)
    : th_(t.th_), join_data_(t.join_data_) {
  t.th_ = nullptr;
  t.join_data_ = nullptr;
}

inline Thread &Thread::operator=(Thread &&t) {
  th_ = t.th_;
  t.th_ = nullptr;
  join_data_ = t.join_data_;
  t.join_data_= nullptr;
  return *this;
}

template <typename F> Thread::Thread(F &&f) {
  auto *heap_header = Runtime::get_current_obj_heap_header();

  if (heap_header) {
    create_in_obj_env(f, heap_header);
  } else {
    create_in_runtime_env(f);
  }
}

static inline void wait_for_migration(HeapHeader *header) {
  header->mutex.lock();
  while (!thread_is_migrated()) {
    header->cond_var.wait(&header->mutex);
  }
  header->mutex.unlock();
}

template <typename F>
void Thread::create_in_obj_env(F &&f, HeapHeader *header) {
  join_data **args;
retry:
  OutermostMigrationDisabledGuard guard(header);
  if (unlikely(!guard)) {
    wait_for_migration(header);
    goto retry;
  }
  th_ = thread_create_with_buf(trampoline_in_obj_env,
                               reinterpret_cast<void **>(&args), sizeof(*args));
  BUG_ON(!th_);
  join_data_ = new join_data(std::forward<F>(f), std::move(guard));
  *args = join_data_;
  thread_ready(th_);
}

template <typename F>
void Thread::create_in_runtime_env(F &&f) {
  join_data **args;
  th_ = thread_create_with_buf(trampoline_in_runtime_env,
                               reinterpret_cast<void **>(&args), sizeof(*args));
  BUG_ON(!th_);
  join_data_ = new join_data(std::forward<F>(f));
  *args = join_data_;
  thread_ready(th_);
}

inline bool Thread::joinable() { return join_data_; }
}
