#include "heap_mgr.hpp"
#include "runtime.hpp"

namespace nu {

inline Mutex::Mutex() {
  auto *heap_header = Runtime::get_obj_heap_header();
  if (heap_header) {
    heap_header->mutexes->put(this);
  }
  mutex_init(&mutex_);
}

inline Mutex::~Mutex() {
  auto *heap_header = Runtime::get_obj_heap_header();
  if (heap_header) {
    heap_header->mutexes->remove(this);
  }
  assert(!mutex_held(&mutex_));
}

inline void Mutex::lock() { mutex_lock(&mutex_); }

inline void Mutex::unlock() { mutex_unlock(&mutex_); }

inline bool Mutex::try_lock() { return mutex_try_lock(&mutex_); }

inline list_head *Mutex::get_waiters() { return &mutex_.waiters; }

} // namespace nu
