extern "C" {
#include <asm/atomic.h>
#include <base/assert.h>
#include <base/compiler.h>
#include <runtime/preempt.h>
#include <runtime/thread.h>
}

#include "utils/rcu_lock.hpp"

namespace nu {

inline void RCULock::detect_sync_barrier() {
  while (unlikely(ACCESS_ONCE(sync_barrier_))) {
    thread_yield();
  }
}

void RCULock::reader_lock() {
  detect_sync_barrier();
  int core = get_cpu();
  Cnt cnt;
  cnt.raw = aligned_cnts_[core].cnt.raw;
  cnt.data.c++;
  cnt.data.ver++;
  aligned_cnts_[core].cnt.data = cnt.data;
  put_cpu();
}

void RCULock::reader_unlock() {
  int core = get_cpu();
  Cnt cnt;
  cnt.raw = aligned_cnts_[core].cnt.raw;
  cnt.data.c--;
  cnt.data.ver++;
  aligned_cnts_[core].cnt.data = cnt.data;
  put_cpu();
}

void RCULock::writer_sync() {
  sync_barrier_ = true;
  barrier();

  int32_t sum;
  int32_t snapshot_vers[kNumCores];
retry:
  sum = 0;
  for (size_t i = 0; i < kNumCores; i++) {
    Cnt cnt;
    cnt.raw = ACCESS_ONCE(aligned_cnts_[i].cnt.raw);
    snapshot_vers[i] = cnt.data.ver;
    sum += cnt.data.c;
  }
  if (sum != 0) {
    thread_yield();
    goto retry;
  }
  for (size_t i = 0; i < kNumCores; i++) {
    if (unlikely(ACCESS_ONCE(aligned_cnts_[i].cnt.data.ver) !=
                 snapshot_vers[i])) {
      goto retry;
    }
  }

  store_release(&sync_barrier_, false);
}

} // namespace nu
