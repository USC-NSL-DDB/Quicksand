#pragma once

extern "C" {
#include <runtime/sync.h>
}

namespace nu {
class Mutex {
public:
  Mutex();
  Mutex(const Mutex &) = delete;
  Mutex &operator=(const Mutex &) = delete;
  ~Mutex();
  void Lock();
  void Unlock();
  bool TryLock();

private:
  mutex_t mutex_;
  friend class CondVar;
  friend class Migrator;

  list_head *get_waiters();
};
} // namespace nu

#include "impl/mutex.ipp"
