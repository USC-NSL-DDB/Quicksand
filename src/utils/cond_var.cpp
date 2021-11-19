#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/spinlock.hpp"

namespace nu {

void CondVar::wait(Mutex *mutex) {
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
  thread_set_self_waiter_info(waiter_info.raw);
  thread_park_and_unlock_np(&condvar_.waiter_lock);
  thread_set_self_waiter_info(0);

  mutex->lock();
}

void CondVar::wait(SpinLock *spin) {
  thread_t *myth;

  assert_spin_lock_held(&spin->spinlock_);
  spin_lock_np(&condvar_.waiter_lock);
  myth = thread_self();
  spin->unlock();
  auto *myth_link = reinterpret_cast<list_node *>(
      reinterpret_cast<uintptr_t>(myth) + thread_link_offset);
  list_add_tail(&condvar_.waiters, myth_link);
  WaiterInfo waiter_info;
  waiter_info.type = WaiterType::kCondVar;
  waiter_info.addr = reinterpret_cast<uint64_t>(this);
  thread_set_self_waiter_info(waiter_info.raw);
  thread_park_and_unlock_np(&condvar_.waiter_lock);
  thread_set_self_waiter_info(0);

  spin->lock();
}

} // namespace nu
