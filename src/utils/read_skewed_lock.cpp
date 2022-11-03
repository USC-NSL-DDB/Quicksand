extern "C" {
#include <base/time.h>
}

#include "nu/utils/read_skewed_lock.hpp"

namespace nu {

void ReadSkewedLock::reader_wait(bool np) {
  if (np) {
    rcu_lock_.reader_unlock_np();
  } else {
    rcu_lock_.reader_unlock();
  }

  // Fast path: using rt::Yield() to wait for the writer.
  auto start_us = microtime();
  do {
    rt::Yield();
  } while (microtime() < start_us + kReaderWaitFastPathMaxUs &&
           unlikely(rt::access_once(writer_barrier_)));

  if (unlikely(rt::access_once(writer_barrier_))) {
    // Slow path: use Mutex + CondVar.
    mutex_.lock();
    while (unlikely(rt::access_once(writer_barrier_))) {
      cond_var_.wait(&mutex_);
    }
    mutex_.unlock();
  }
}

}  // namespace nu
