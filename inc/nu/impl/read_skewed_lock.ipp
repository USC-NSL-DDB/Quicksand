#include <sync.h>

namespace nu {

inline ReadSkewedLock::ReadSkewedLock() : writer_barrier_(false) {}

inline void ReadSkewedLock::reader_lock() {
retry:
  rcu_lock_.reader_lock();
  if (unlikely(rt::access_once(writer_barrier_))) {
    reader_wait();
    goto retry;
  }
}

inline void ReadSkewedLock::reader_unlock() { rcu_lock_.reader_unlock(); }

inline void ReadSkewedLock::writer_lock() {
  mutex_.lock();
  rt::access_once(writer_barrier_) = true;
  rcu_lock_.writer_sync();
}

inline void ReadSkewedLock::writer_unlock() {
  rt::access_once(writer_barrier_) = false;
  cond_var_.signal_all();
  mutex_.unlock();
}

}  // namespace nu
