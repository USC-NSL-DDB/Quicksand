extern "C" {
#include <runtime/timer.h>
}

#include "nu/utils/rcu_lock.hpp"

namespace nu {

RCULock::RCULock() : sync_barrier_(false) {
  for (uint32_t i = 0; i < kNumCores; i++) {
    per_core_data_[i].cnt.raw = 0;
  }
}

RCULock::~RCULock() {
#ifdef DEBUG
  int32_t sum = 0;
  for (size_t i = 0; i < kNumCores; i++) {
    Cnt cnt;
    cnt.raw = ACCESS_ONCE(per_core_data_[i].cnt.raw);
    sum += cnt.data.c;
  }
  assert(sum == 0);
#endif
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
    cnt.raw = ACCESS_ONCE(per_core_data_[i].cnt.raw);
    snapshot_vers[i] = cnt.data.ver;
    sum += cnt.data.c;
  }
  if (sum != 0) {
    rt::Yield();
    goto retry;
  }
  for (size_t i = 0; i < kNumCores; i++) {
    if (unlikely(ACCESS_ONCE(per_core_data_[i].cnt.data.ver) !=
                 snapshot_vers[i])) {
      goto retry;
    }
  }

  rt::access_once(sync_barrier_) = false;
  for (uint32_t i = 0; i < kNumCores; i++) {
    auto &per_core_data = per_core_data_[i];
    rt::SpinGuard guard(&per_core_data.spin);
    for (auto &waker : per_core_data.wakers) {
      waker.Wake();
    }
    per_core_data.wakers.clear();
  }
}

void RCULock::__detect_sync_barrier() {
  auto &per_core_data = per_core_data_[read_cpu()];
  rt::SpinGuardAndPark guard_and_park(&per_core_data.spin);
  if (likely(rt::access_once(sync_barrier_))) {
    per_core_data.wakers.emplace_back();
    per_core_data.wakers.back().Arm();
  } else {
    guard_and_park.reset();
  }
}

} // namespace nu
