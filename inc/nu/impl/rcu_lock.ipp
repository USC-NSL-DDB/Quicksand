extern "C" {
#include <asm/atomic.h>
#include <base/assert.h>
#include <base/compiler.h>
#include <runtime/preempt.h>
}

#include <thread.h>

namespace nu {

inline RCULock::Result RCULock::reader_lock() {
  bool just_held = thread_hold_rcu(this);
  if (unlikely(just_held && rt::access_once(sync_barrier_))) {
    reader_wait();
  }
  __reader_lock();
  return just_held ? Result::Succeed : Result::Already;
}

inline RCULock::Result RCULock::try_reader_lock() {
  bool just_held = thread_hold_rcu(this);
  if (unlikely(just_held && rt::access_once(sync_barrier_))) {
    thread_unhold_rcu(this);
    return Result::Failed;
  }
  __reader_lock();
  return just_held ? Result::Succeed : Result::Already;
}

} // namespace nu
