#include "nu/heap_mgr.hpp"
#include "nu/runtime.hpp"

namespace nu {

inline Mutex::Mutex() {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  if (heap_header) {
    heap_header->mutexes->put(this);
  }
  mutex_init(&mutex_);
}

inline Mutex::~Mutex() {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  if (heap_header) {
    heap_header->mutexes->remove(this);
  }
  assert(!mutex_held(&mutex_));
}

inline void Mutex::Lock() { mutex_lock(&mutex_); }

inline void Mutex::Unlock() { mutex_unlock(&mutex_); }

inline bool Mutex::TryLock() { return mutex_try_lock(&mutex_); }

inline list_head *Mutex::get_waiters() { return &mutex_.waiters; }

} // namespace nu
