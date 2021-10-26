#include <sync.h>

namespace nu {

inline Mutex::Mutex() {
  mutex_init(&mutex_);
}

inline Mutex::~Mutex() {
  assert(!mutex_held(&mutex_));
}

inline void Mutex::lock() {
  if (unlikely(!try_lock())) {
    WaiterInfo waiter_info;
    waiter_info.type = WaiterType::kMutex;
    waiter_info.addr = reinterpret_cast<uint64_t>(this);
    set_self_waiter_info(waiter_info.raw);
    __mutex_lock(&mutex_);
    set_self_waiter_info(0);
  }
}

inline void Mutex::unlock() {
  rt::Preempt p;
  rt::PreemptGuard g(&p);
  mutex_unlock(&mutex_);
}

inline bool Mutex::try_lock() { return mutex_try_lock(&mutex_); }

inline list_head *Mutex::get_waiters() { return &mutex_.waiters; }

} // namespace nu
