#include "nu/utils/mutex.hpp"

namespace nu {

inline CondVar::CondVar() { condvar_init(&condvar_); }

inline CondVar::~CondVar() {}

inline void CondVar::wait(Mutex *mutex) {
  thread_t *myth;

  assert_mutex_held(&mutex->mutex_);
  spin_lock_np(&condvar_.waiter_lock);
  myth = thread_self();
  mutex->unlock();
  auto *myth_link = reinterpret_cast<list_node *>(
      reinterpret_cast<uintptr_t>(myth) + thread_link_offset);
  list_add_tail(&condvar_.waiters, myth_link);
  WaiterInfo waiter_info;
  waiter_info.type = WaiterType::kCondVar;
  waiter_info.addr = reinterpret_cast<uint64_t>(this);
  set_self_waiter_info(waiter_info.raw);
  thread_park_and_unlock_np(&condvar_.waiter_lock);
  set_self_waiter_info(0);

  mutex->lock();
}

inline void CondVar::signal() {
  rt::Preempt p;
  rt::PreemptGuard g(&p);
  condvar_signal(&condvar_);
}

inline void CondVar::signal_all() {
  rt::Preempt p;
  rt::PreemptGuard g(&p);
  condvar_broadcast(&condvar_);
}

inline list_head *CondVar::get_waiters() { return &condvar_.waiters; }

} // namespace nu
