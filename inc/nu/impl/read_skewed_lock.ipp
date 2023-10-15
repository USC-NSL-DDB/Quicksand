#include <nu/utils/caladan.hpp>

namespace nu {

inline ReadSkewedLock::ReadSkewedLock() : writer_barrier_(false) {}

inline void ReadSkewedLock::reader_lock() {
retry:
  rcu_lock_.reader_lock();
  if (unlikely(Caladan::access_once(writer_barrier_))) {
    reader_wait();
    goto retry;
  }
}

inline bool ReadSkewedLock::reader_try_lock() {
  rcu_lock_.reader_lock();
  if (unlikely(Caladan::access_once(writer_barrier_))) {
    rcu_lock_.reader_unlock();
    return false;
  }
  return true;
}

inline void ReadSkewedLock::reader_unlock() { rcu_lock_.reader_unlock(); }

inline void ReadSkewedLock::writer_lock() {
  writer_mutex_.lock();
  Caladan::access_once(writer_barrier_) = true;
  rcu_lock_.writer_sync();
}

inline bool ReadSkewedLock::writer_lock_if(std::function<bool()> cond) {
  writer_mutex_.lock();
  bool ret = cond();  // The first check to handle concurrent writers.
  if (ret) {
    Caladan::access_once(writer_barrier_) = true;
    rcu_lock_.writer_sync();
    ret = cond(); // The second check to handle concurrent reader/writer.
    if (unlikely(!ret)) {
      writer_unlock();
    }
  } else {
    writer_mutex_.unlock();
  }
  return ret;
}

inline void ReadSkewedLock::writer_unlock() {
  reader_spin_.lock();
  Caladan::access_once(writer_barrier_) = false;
  reader_spin_.unlock();
  cond_var_.signal_all();
  writer_mutex_.unlock();
}

}  // namespace nu
