extern "C" {
#include <runtime/sync.h>
#include <runtime/thread.h>
}

#include "nu/runtime.hpp"
#include "nu/utils/blocked_syncer.hpp"
#include "nu/utils/mutex.hpp"

namespace nu {

constexpr uint32_t kWaiterFlag = 1 << 31;

void Mutex::__lock() {
  thread_t *myth;

  spin_lock_np(&m_.waiter_lock);

  /* did we race with mutex_unlock? */
  if (atomic_fetch_and_or(&m_.held, kWaiterFlag) == 0) {
    atomic_write(&m_.held, 1);
    spin_unlock_np(&m_.waiter_lock);
    return;
  }

  myth = thread_self();
  if (list_empty(&m_.waiters)) {
    auto *proclet_header = Runtime::get_current_proclet_header();
    if (proclet_header) {
      proclet_header->blocked_syncer.add(this, BlockedSyncer::Type::kMutex);
    }
  }
  auto *myth_link = reinterpret_cast<list_node *>(
      reinterpret_cast<uintptr_t>(myth) + thread_link_offset);
  list_add_tail(&m_.waiters, myth_link);
  thread_park_and_unlock_np(&m_.waiter_lock);
}

void Mutex::__unlock() {
  rt::Preempt p;
  rt::PreemptGuard g(&p);

  thread_t *waketh;

  spin_lock_np(&m_.waiter_lock);

  waketh = reinterpret_cast<thread_t *>(
      const_cast<void *>(list_pop_(&m_.waiters, thread_link_offset)));
  if (!waketh) {
    atomic_write(&m_.held, 0);
    auto *proclet_header = Runtime::get_current_proclet_header();
    if (proclet_header) {
      proclet_header->blocked_syncer.remove(this);
    }
    spin_unlock_np(&m_.waiter_lock);
    return;
  }
  spin_unlock_np(&m_.waiter_lock);
  thread_ready(waketh);
}

}  // namespace nu
