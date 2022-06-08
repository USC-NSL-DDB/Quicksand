#include <sync.h>

namespace nu {

inline CondVar::CondVar() { condvar_init(&cv_); }

inline CondVar::~CondVar() {}

inline list_head *CondVar::get_waiters() { return &cv_.waiters; }

}  // namespace nu
