#pragma once

#include "nu/commons.hpp"

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
  void lock();
  void unlock();
  bool try_lock();

private:
  mutex_t m_;
  friend class CondVar;
  friend class Migrator;

  list_head *get_waiters();
  void __lock();
  void __unlock();
};
} // namespace nu

#include "nu/impl/mutex.ipp"
