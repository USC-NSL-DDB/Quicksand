#include "nu/utils/cond_var.hpp"

#include "nu/runtime.hpp"
#include "nu/utils/blocked_syncer.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/spin_lock.hpp"

namespace nu {

void CondVar::__wait_and_unlock(auto *l) {
  Caladan::spin_lock_np(&cv_.waiter_lock);
  l->unlock();

  if (list_empty(&cv_.waiters)) {
    auto *proclet_header = get_runtime()->get_current_proclet_header();
    if (proclet_header) {
      proclet_header->blocked_syncer.add(this, BlockedSyncer::Type::kCondVar);
    }
  }

  get_runtime()->caladan()->thread_park_and_unlock_np(&cv_.waiter_lock,
                                                      &cv_.waiters);

  if (unlikely(list_empty(&cv_.waiters))) {
    Caladan::spin_lock_np(&cv_.waiter_lock);
    if (likely(list_empty(&cv_.waiters))) {
      auto *proclet_header = get_runtime()->get_current_proclet_header();
      if (proclet_header) {
        proclet_header->blocked_syncer.remove(this);
      }
    }
    Caladan::spin_unlock_np(&cv_.waiter_lock);
  }
}

void CondVar::wait(Mutex *mutex) {
  wait_and_unlock(mutex);
  mutex->lock();
}

void CondVar::wait_and_unlock(Mutex *mutex) {
  assert_mutex_held(&mutex->m_);
  __wait_and_unlock(mutex);
}

void CondVar::wait(SpinLock *spin) {
  __wait_and_unlock(spin);
  spin->lock();
}

void CondVar::wait_and_unlock(SpinLock *spin) {
  assert_spin_lock_held(&spin->spinlock_);
  __wait_and_unlock(spin);
}

void CondVar::signal() {
  Caladan::spin_lock_np(&cv_.waiter_lock);
  get_runtime()->caladan()->wake_one_thread(&cv_.waiters);
  Caladan::spin_unlock_np(&cv_.waiter_lock);
}

void CondVar::signal_all() {
  Caladan::spin_lock_np(&cv_.waiter_lock);
  get_runtime()->caladan()->wake_all_threads(&cv_.waiters);
  Caladan::spin_unlock_np(&cv_.waiter_lock);
}

}  // namespace nu
