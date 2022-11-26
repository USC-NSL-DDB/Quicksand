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
  auto *th = get_runtime()->caladan()->pop_one_waiter(&cv_.waiters);
  if (th) {
    if (unlikely(list_empty(&cv_.waiters))) {
      auto *proclet_header =
          get_runtime()->caladan()->thread_get_owner_proclet(th);
      if (proclet_header) {
        proclet_header->blocked_syncer.remove(this);
      }
    }
    get_runtime()->caladan()->thread_ready(th);
  }
  Caladan::spin_unlock_np(&cv_.waiter_lock);
}

void CondVar::signal_all() {
  Caladan::spin_lock_np(&cv_.waiter_lock);
   auto ths = get_runtime()->caladan()->pop_all_waiters(&cv_.waiters);
   if (!ths.empty()) {
     auto *proclet_header =
         get_runtime()->caladan()->thread_get_owner_proclet(ths.front());
#ifdef DEBUG
     for (auto *th : ths) {
       BUG_ON(proclet_header !=
              get_runtime()->caladan()->thread_get_owner_proclet(th));
     }
#endif
     if (proclet_header) {
       proclet_header->blocked_syncer.remove(this);
     }
     for (auto *th : ths) {
       get_runtime()->caladan()->thread_ready(th);
     }
   }
   Caladan::spin_unlock_np(&cv_.waiter_lock);
}

}  // namespace nu
