extern "C" {
#include <asm/atomic.h>
#include <base/assert.h>
#include <base/compiler.h>
#include <runtime/preempt.h>
}

#include <thread.h>

namespace nu {

inline void RCULock::reader_lock() {
  reader_lock_np();
  put_cpu();
}

inline void RCULock::reader_lock_np() {
  thread_hold_rcu(this);
  if (unlikely(rt::access_once(sync_barrier_))) {
    reader_wait();
  }
  __reader_lock_np();
}

inline bool RCULock::try_reader_lock() {
  if (try_reader_lock_np()) {
    put_cpu();
    return true;
  }
  return false;
}

inline bool RCULock::try_reader_lock_np() {
  thread_hold_rcu(this);
  if (unlikely(rt::access_once(sync_barrier_))) {
    thread_unhold_rcu(this);
    return false;
  }
  __reader_lock_np();
  return true;
}

}  // namespace nu
