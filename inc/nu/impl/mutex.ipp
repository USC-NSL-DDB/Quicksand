extern "C" {
#include <base/atomic.h>
}

#include <sync.h>

namespace nu {

inline Mutex::Mutex() {
  mutex_init(&m_);
}

inline Mutex::~Mutex() {
  assert(!mutex_held(&m_));
}

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

inline bool Mutex::try_lock() { return mutex_try_lock(&m_); }

inline list_head *Mutex::get_waiters() { return &m_.waiters; }

} // namespace nu
