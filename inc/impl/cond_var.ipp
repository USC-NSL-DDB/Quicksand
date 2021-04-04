#include "heap_mgr.hpp"
#include "mutex.hpp"
#include "runtime.hpp"

namespace nu {

inline CondVar::CondVar() {
  auto *heap_header = Runtime::get_obj_heap_header();
  if (heap_header) {
    heap_header->condvars->put(this);
  }
  condvar_init(&condvar_);
}

inline CondVar::~CondVar() {
  auto *heap_header = Runtime::get_obj_heap_header();
  if (heap_header) {
    heap_header->condvars->remove(this);
  }
}

inline void CondVar::wait(Mutex *mutex) { condvar_wait(&condvar_, &mutex->mutex_); }

inline void CondVar::signal() { condvar_signal(&condvar_); }

inline void CondVar::signal_all() { condvar_broadcast(&condvar_); }

inline list_head *CondVar::get_waiters() { return &condvar_.waiters; }

} // namespace nu
