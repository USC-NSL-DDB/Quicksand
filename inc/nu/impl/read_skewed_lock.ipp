#include <sync.h>

namespace nu {

inline ReadSkewedLock::ReadSkewedLock() : writer_barrier_(false) {}

inline void ReadSkewedLock::reader_unlock() { rcu_lock_.reader_unlock(); }

inline void ReadSkewedLock::reader_unlock_np() {
  rcu_lock_.reader_unlock_np();
}

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

inline void ReadSkewedLock::reader_lock() {
retry:
  rcu_lock_.reader_lock();
  if (unlikely(rt::access_once(writer_barrier_))) {
    reader_wait(false);
    goto retry;
  }
}

inline void ReadSkewedLock::reader_lock_np() {
retry:
  rcu_lock_.reader_lock_np();
  if (unlikely(rt::access_once(writer_barrier_))) {
    reader_wait(true);
    goto retry;
  }
}

}  // namespace nu
