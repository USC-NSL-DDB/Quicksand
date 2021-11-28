extern "C" {
#include <runtime/timer.h>
}

#include "nu/utils/rcu_lock.hpp"

namespace nu {

RCULock::RCULock() : sync_barrier_(false) {
  memset(aligned_cnts_, 0, sizeof(aligned_cnts_));
}

RCULock::~RCULock() {
#ifdef DEBUG
  int32_t sum = 0;
  for (size_t i = 0; i < kNumCores; i++) {
    Cnt cnt;
    cnt.raw = ACCESS_ONCE(aligned_cnts_[i].cnt.raw);
    sum += cnt.data.c;
  }
  assert(sum == 0);
#endif
}

void RCULock::writer_sync(bool prioritize_readers) {
  sync_barrier_ = true;
  barrier();

  int32_t sum;
  int32_t snapshot_vers[kNumCores];
  auto start_us = microtime();
  bool prioritized = false;

retry:
  sum = 0;
  for (size_t i = 0; i < kNumCores; i++) {
    Cnt cnt;
    cnt.raw = ACCESS_ONCE(aligned_cnts_[i].cnt.raw);
    snapshot_vers[i] = cnt.data.ver;
    sum += cnt.data.c;
  }
  if (sum != 0) {
    if (prioritize_readers) {
      if (!prioritized) {
        prioritize_rcu_readers(this);
        prioritized = true;
      }
    } else {
      if (likely(microtime() < start_us + kWriterWaitFastPathMaxUs)) {
        // Fast path.
        rt::Yield();
      } else {
        // Slow path.
        timer_sleep(kWriterWaitSlowPathSleepUs);
      }
    }
    goto retry;
  }
  for (size_t i = 0; i < kNumCores; i++) {
    if (unlikely(ACCESS_ONCE(aligned_cnts_[i].cnt.data.ver) !=
                 snapshot_vers[i])) {
      goto retry;
    }
  }

  rt::access_once(sync_barrier_) = false;
  rt::SpinGuard guard(&spin_);
  for (auto &waker : wakers_) {
    waker.Wake();
  }
  wakers_.clear();
}

void RCULock::reader_wait() {
  // Fast path.
  auto start_us = microtime();
  do {
    rt::Yield();
  } while (microtime() < start_us + kReaderWaitFastPathMaxUs &&
           unlikely(rt::access_once(sync_barrier_)));

  if (unlikely(rt::access_once(sync_barrier_))) {
    // Slow path.
    rt::SpinGuard spin_guard(&spin_);
    if (likely(rt::access_once(sync_barrier_))) {
      wakers_.emplace_back();
      spin_guard.Park(&wakers_.back());
    }
  }
}

void RCULock::__reader_lock() {
  int core = get_cpu();
  Cnt cnt;
  cnt.raw = aligned_cnts_[core].cnt.raw;
  cnt.data.c++;
  cnt.data.ver++;
  aligned_cnts_[core].cnt.data = cnt.data;
  put_cpu();
}

void RCULock::reader_unlock() {
  thread_unhold_rcu(this);
  int core = get_cpu();
  Cnt cnt;
  cnt.raw = aligned_cnts_[core].cnt.raw;
  cnt.data.c--;
  cnt.data.ver++;
  aligned_cnts_[core].cnt.data = cnt.data;
  put_cpu();
}

} // namespace nu
