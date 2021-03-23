extern "C" {
#include <asm/atomic.h>
#include <base/assert.h>
#include <runtime/preempt.h>
#include <runtime/thread.h>
}

namespace nu {

inline void RCULock::lock() {
  while (unlikely(ACCESS_ONCE(sync_barrier_))) {
    thread_yield();
  }
  int core = get_cpu();
  Cnt cnt;
  cnt.raw = aligned_cnts_[core].cnt.raw;
  cnt.data.c++;
  cnt.data.ver++;
  aligned_cnts_[core].cnt.data = cnt.data;
  put_cpu();
}

inline void RCULock::unlock() {
  int core = get_cpu();
  Cnt cnt;
  cnt.raw = aligned_cnts_[core].cnt.raw;
  cnt.data.c--;
  cnt.data.ver++;
  aligned_cnts_[core].cnt.data = cnt.data;
  put_cpu();
}

} // namespace nu
