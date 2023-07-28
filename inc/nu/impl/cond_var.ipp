#include "nu/utils/caladan.hpp"

namespace nu {

inline CondVar::CondVar() { Caladan::condvar_init(&cv_); }

inline CondVar::~CondVar() { BUG_ON(!list_empty(&cv_.waiters)); }

inline list_head *CondVar::get_waiters() { return &cv_.waiters; }

}  // namespace nu
