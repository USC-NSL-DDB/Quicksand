#include <sync.h>

namespace nu {

inline CondVar::CondVar() { condvar_init(&condvar_); }

inline CondVar::~CondVar() {}

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
