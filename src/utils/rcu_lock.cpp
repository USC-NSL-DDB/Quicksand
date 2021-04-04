#include "utils/rcu_lock.hpp"

namespace nu {

void RCULock::synchronize() {
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
  barrier();
  sync_barrier_ = false;
}

} // namespace nu
