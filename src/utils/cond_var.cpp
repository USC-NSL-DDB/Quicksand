#include "nu/utils/cond_var.hpp"

#include "nu/runtime.hpp"
#include "nu/utils/blocked_syncer.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/spin_lock.hpp"

namespace nu {

void CondVar::wait(Mutex *mutex) {
  assert_mutex_held(&mutex->m_);
  Caladan::spin_lock_np(&cv_.waiter_lock);
  mutex->unlock();
  if (list_empty(&cv_.waiters)) {
    auto *proclet_header = get_runtime()->get_current_proclet_header();
    if (proclet_header) {
      proclet_header->blocked_syncer.add(this, BlockedSyncer::Type::kCondVar);
    }
  }

  get_runtime()->caladan()->thread_park_and_unlock_np(&cv_.waiter_lock,
                                                      &cv_.waiters);

  mutex->lock();
}

void CondVar::wait(SpinLock *spin) {
  wait_and_unlock(spin);
  spin->lock();
}

void CondVar::wait_and_unlock(SpinLock *spin) {
  assert_spin_lock_held(&spin->spinlock_);
  Caladan::spin_lock_np(&cv_.waiter_lock);
  spin->unlock();
  if (list_empty(&cv_.waiters)) {
    auto *proclet_header = get_runtime()->get_current_proclet_header();
    if (proclet_header) {
      proclet_header->blocked_syncer.add(this, BlockedSyncer::Type::kCondVar);
    }
  }

  get_runtime()->caladan()->thread_park_and_unlock_np(&cv_.waiter_lock,
                                                      &cv_.waiters);
}

void CondVar::signal() {
  Caladan::spin_lock_np(&cv_.waiter_lock);
  if (!list_empty(&cv_.waiters)) {
    get_runtime()->caladan()->wake_one_thread(&cv_.waiters);
    if (unlikely(list_empty(&cv_.waiters))) {
      auto *proclet_header = get_runtime()->get_current_proclet_header();
      if (proclet_header) {
        proclet_header->blocked_syncer.remove(this);
      }
    }
  }
  Caladan::spin_unlock_np(&cv_.waiter_lock);
}

void CondVar::signal_all() {
  Caladan::spin_lock_np(&cv_.waiter_lock);
  if (!list_empty(&cv_.waiters)) {
    auto *proclet_header = get_runtime()->get_current_proclet_header();
    if (proclet_header) {
      proclet_header->blocked_syncer.remove(this);
    }
  }
  get_runtime()->caladan()->wake_all_threads(&cv_.waiters);
  Caladan::spin_unlock_np(&cv_.waiter_lock);
}

}  // namespace nu
