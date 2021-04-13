#include "utils/rcu_lock.hpp"

namespace nu {

template <typename Fn> void RCULock::write_sync_general(Fn &&fn) {
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

  fn();

  barrier();
  sync_barrier_ = false;
}

void RCULock::writer_sync() {
  write_sync_general([] {});
}

void RCULock::writer_sync_fn(const std::function<void(void)> &fn) {
  write_sync_general(fn);
}

void RCULock::writer_sync_fn(std::function<void(void)> &&fn) {
  write_sync_general(fn);
}

} // namespace nu
