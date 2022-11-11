#include "nu/utils/cond_var.hpp"

#include "nu/runtime.hpp"
#include "nu/utils/blocked_syncer.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/spin_lock.hpp"

namespace nu {

void CondVar::wait(Mutex *mutex) {
  thread_t *myth;

  assert_mutex_held(&mutex->m_);
  spin_lock_np(&cv_.waiter_lock);
  myth = thread_self();
  mutex->unlock();
  if (list_empty(&cv_.waiters)) {
    auto *proclet_header = get_runtime()->get_current_proclet_header();
    if (proclet_header) {
      proclet_header->blocked_syncer.add(this, BlockedSyncer::Type::kCondVar);
    }
  }
  auto *myth_link = reinterpret_cast<list_node *>(
      reinterpret_cast<uintptr_t>(myth) + thread_link_offset);
  list_add_tail(&cv_.waiters, myth_link);
  thread_park_and_unlock_np(&cv_.waiter_lock);

  mutex->lock();
}

void CondVar::wait(SpinLock *spin) {
  wait_and_unlock(spin);
  spin->lock();
}

void CondVar::wait_and_unlock(SpinLock *spin) {
  thread_t *myth;

  assert_spin_lock_held(&spin->spinlock_);
  spin_lock_np(&cv_.waiter_lock);
  myth = thread_self();
  spin->unlock();
  if (list_empty(&cv_.waiters)) {
    auto *proclet_header = get_runtime()->get_current_proclet_header();
    if (proclet_header) {
      proclet_header->blocked_syncer.add(this, BlockedSyncer::Type::kCondVar);
    }
  }
  auto *myth_link = reinterpret_cast<list_node *>(
      reinterpret_cast<uintptr_t>(myth) + thread_link_offset);
  list_add_tail(&cv_.waiters, myth_link);
  thread_park_and_unlock_np(&cv_.waiter_lock);
}

void CondVar::signal() {
  rt::Preempt p;
  rt::PreemptGuard g(&p);

  thread_t *waketh;

  spin_lock_np(&cv_.waiter_lock);
  waketh = reinterpret_cast<thread_t *>(
      const_cast<void *>(list_pop_(&cv_.waiters, thread_link_offset)));
  if (waketh && unlikely(list_empty(&cv_.waiters))) {
    auto *proclet_header = get_runtime()->get_current_proclet_header();
    if (proclet_header) {
      proclet_header->blocked_syncer.remove(this);
    }
  }
  spin_unlock_np(&cv_.waiter_lock);
  if (waketh) {
    thread_ready(waketh);
  }
}

void CondVar::signal_all() {
  rt::Preempt p;
  rt::PreemptGuard g(&p);

  thread_t *waketh;
  struct list_head tmp;

  list_head_init(&tmp);

  spin_lock_np(&cv_.waiter_lock);
  if (!list_empty(&cv_.waiters)) {
    auto *proclet_header = get_runtime()->get_current_proclet_header();
    if (proclet_header) {
      proclet_header->blocked_syncer.remove(this);
    }
  }
  list_append_list(&tmp, &cv_.waiters);
  spin_unlock_np(&cv_.waiter_lock);

  while (true) {
    waketh = reinterpret_cast<thread_t *>(
        const_cast<void *>(list_pop_(&tmp, thread_link_offset)));
    if (!waketh) {
      break;
    }
    thread_ready(waketh);
  }
}

}  // namespace nu
