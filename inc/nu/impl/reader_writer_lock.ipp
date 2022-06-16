#include <sync.h>

namespace nu {

inline ReaderWriterLock::ReaderWriterLock() : writer_barrier_(false) {}

inline void ReaderWriterLock::reader_unlock() { rcu_lock_.reader_unlock(); }

inline void ReaderWriterLock::writer_lock() {
  mutex_.lock();
  rt::access_once(writer_barrier_) = true;
  rcu_lock_.writer_sync();
}

inline void ReaderWriterLock::writer_unlock() {
  rt::access_once(writer_barrier_) = false;
  cond_var_.signal_all();
  mutex_.unlock();
}
}  // namespace nu
