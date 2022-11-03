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

inline void RCULock::reader_unlock() {
  get_cpu();
  reader_unlock_np();
}

inline void RCULock::reader_lock_np() {
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
  if (unlikely(rt::access_once(sync_barrier_))) {
    return false;
  }
  __reader_lock_np();
  return true;
}

inline void RCULock::__reader_lock_np() {
  int core = get_cpu();
  Cnt cnt;
  cnt.raw = aligned_cnts_[core].cnt.raw;
  cnt.data.c++;
  cnt.data.ver++;
  aligned_cnts_[core].cnt.data = cnt.data;
  barrier();
  thread_hold_rcu(this);
}

inline void RCULock::reader_unlock_np() {
  thread_unhold_rcu(this);
  int core = read_cpu();
  Cnt cnt;
  cnt.raw = aligned_cnts_[core].cnt.raw;
  cnt.data.c--;
  cnt.data.ver++;
  barrier();
  aligned_cnts_[core].cnt.data = cnt.data;
  put_cpu();
}

}  // namespace nu
