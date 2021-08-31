extern "C" {
#include <asm/atomic.h>
#include <base/assert.h>
#include <base/compiler.h>
#include <runtime/preempt.h>
}

#include <sync.h>
#include <thread.h>

namespace nu {

// TODO: fix the potential live lock in which the writer core was ceded.
inline void RCULock::detect_sync_barrier() {
  while (unlikely(rt::access_once(sync_barrier_))) {
    rt::Yield();
  }
}

inline void RCULock::reader_lock() {
  detect_sync_barrier();
  int core = get_cpu();
  Cnt cnt;
  cnt.raw = aligned_cnts_[core].cnt.raw;
  cnt.data.c++;
  cnt.data.ver++;
  aligned_cnts_[core].cnt.data = cnt.data;
  put_cpu();
}

inline bool RCULock::try_reader_lock() {
  if (unlikely(rt::access_once(sync_barrier_))) {
    return false;
  }
  int core = get_cpu();
  Cnt cnt;
  cnt.raw = aligned_cnts_[core].cnt.raw;
  cnt.data.c++;
  cnt.data.ver++;
  aligned_cnts_[core].cnt.data = cnt.data;
  put_cpu();

  return true;
}

inline void RCULock::reader_unlock() {
  int core = get_cpu();
  Cnt cnt;
  cnt.raw = aligned_cnts_[core].cnt.raw;
  cnt.data.c--;
  cnt.data.ver++;
  aligned_cnts_[core].cnt.data = cnt.data;
  put_cpu();
}

} // namespace nu