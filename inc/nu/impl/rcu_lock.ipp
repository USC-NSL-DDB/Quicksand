extern "C" {
#include <asm/atomic.h>
#include <base/assert.h>
#include <base/compiler.h>
#include <runtime/preempt.h>
}

#include <thread.h>

namespace nu {

inline void RCULock::detect_sync_barrier() {
  if (unlikely(rt::access_once(sync_barrier_))) {
    __detect_sync_barrier();
  }
}

inline void RCULock::reader_lock() {
  detect_sync_barrier(); // Avoid starvation.
  int core = get_cpu();
  Cnt cnt;
  cnt.raw = per_core_data_[core].cnt.raw;
  cnt.data.c++;
  cnt.data.ver++;
  per_core_data_[core].cnt.data = cnt.data;
  put_cpu();
}

inline bool RCULock::try_reader_lock() {
  if (unlikely(rt::access_once(sync_barrier_))) {
    return false;
  }
  int core = get_cpu();
  Cnt cnt;
  cnt.raw = per_core_data_[core].cnt.raw;
  cnt.data.c++;
  cnt.data.ver++;
  per_core_data_[core].cnt.data = cnt.data;
  put_cpu();

  return true;
}

inline void RCULock::reader_unlock() {
  int core = get_cpu();
  Cnt cnt;
  cnt.raw = per_core_data_[core].cnt.raw;
  cnt.data.c--;
  cnt.data.ver++;
  per_core_data_[core].cnt.data = cnt.data;
  put_cpu();
}

} // namespace nu
