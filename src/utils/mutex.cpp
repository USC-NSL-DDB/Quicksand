extern "C" {
#include <runtime/thread.h>
}

#include "nu/runtime.hpp"
#include "nu/utils/blocked_syncer.hpp"
#include "nu/utils/mutex.hpp"

namespace nu {

constexpr uint32_t kWaiterFlag = 1 << 31;

void Mutex::__lock() {
  Caladan::spin_lock_np(&m_.waiter_lock);

  /* did we race with mutex_unlock? */
  if (atomic_fetch_and_or(&m_.held, kWaiterFlag) == 0) {
    atomic_write(&m_.held, 1);
    Caladan::spin_unlock_np(&m_.waiter_lock);
    return;
  }

  if (list_empty(&m_.waiters)) {
    auto *proclet_header = get_runtime()->get_current_proclet_header();
    if (proclet_header) {
      proclet_header->blocked_syncer.add(this, BlockedSyncer::Type::kMutex);
    }
  }

  get_runtime()->caladan()->thread_park_and_unlock_np(&m_.waiter_lock,
                                                      &m_.waiters);
}

void Mutex::__unlock() {
  Caladan::spin_lock_np(&m_.waiter_lock);

  if (list_empty(&m_.waiters)) {
    atomic_write(&m_.held, 0);
    auto *proclet_header = get_runtime()->get_current_proclet_header();
    if (proclet_header) {
      proclet_header->blocked_syncer.remove(this);
    }
  } else {
    get_runtime()->caladan()->wake_one_thread(&m_.waiters);
  }

  Caladan::spin_unlock_np(&m_.waiter_lock);
}

}  // namespace nu
