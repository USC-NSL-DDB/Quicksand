#include "nu/utils/caladan.hpp"

namespace nu {

inline Mutex::Mutex() { Caladan::mutex_init(&m_); }

inline Mutex::~Mutex() { assert(!Caladan::mutex_held(&m_)); }

inline void Mutex::lock() {
  if (unlikely(!try_lock())) {
    __lock();
  }
}

inline void Mutex::unlock() {
  if (likely(atomic_cmpxchg(&m_.held, 1, 0))) {
    return;
  }
  __unlock();
}

inline bool Mutex::try_lock() { return Caladan::mutex_try_lock(&m_); }

inline list_head *Mutex::get_waiters() { return &m_.waiters; }

}  // namespace nu
