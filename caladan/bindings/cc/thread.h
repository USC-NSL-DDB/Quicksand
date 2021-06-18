// thread.h - Support for creating and managing threads

#pragma once

extern "C" {
#include <base/assert.h>
#include <runtime/sync.h>
}

#include <folly/Function.h>

namespace rt {
namespace thread_internal {

struct join_data {
  template <typename F>
  join_data(F &&f) : done_(false), waiter_(nullptr), func_(std::forward<F>(f)) {
    spin_lock_init(&lock_);
  }

  spinlock_t lock_;
  bool done_;
  thread_t *waiter_;
  folly::Function<void()> func_;
};

extern void ThreadTrampoline(void *arg);
extern void ThreadTrampolineWithJoin(void *arg);

}  // namespace thread_internal

// Spawns a new thread.
template <typename F> void Spawn(F &&f) {
  void *buf;
  thread_t *th = thread_create_with_buf(thread_internal::ThreadTrampoline, &buf,
                                        sizeof(folly::Function<void()>));
  if (unlikely(!th)) BUG();
  new (buf) folly::Function<void()>(std::forward<F>(f));
  thread_ready(th);
}

// Called from a running thread to exit.
inline void Exit(void) { thread_exit(); }

// Called from a running thread to yield.
inline void Yield(void) { thread_yield(); }

// A C++11 style thread class
class Thread {
 public:
  // boilerplate constructors.
  Thread() : join_data_(nullptr) {}
  ~Thread();

  // disable copy.
  Thread(const Thread &) = delete;
  Thread &operator=(const Thread &) = delete;

  // Move support.
  Thread(Thread &&t) : join_data_(t.join_data_) { t.join_data_ = nullptr; }
  Thread &operator=(Thread &&t) {
    join_data_ = t.join_data_;
    t.join_data_ = nullptr;
    return *this;
  }

  // Spawns a thread.
  template <typename F> Thread(F&& f) {
    thread_internal::join_data *buf;
    thread_t *th =
        thread_create_with_buf(thread_internal::ThreadTrampolineWithJoin,
                               reinterpret_cast<void **>(&buf), sizeof(*buf));
    if (unlikely(!th))
      BUG();
    new (buf) thread_internal::join_data(std::forward<F>(f));
    join_data_ = buf;
    thread_ready(th);
  }

  // Waits for the thread to exit.
  void Join();

  // Detaches the thread, indicating it won't be joined in the future.
  void Detach();

 private:
  thread_internal::join_data* join_data_;
};

}  // namespace rt
