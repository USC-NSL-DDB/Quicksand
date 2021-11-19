#pragma once

#include <sync.h>

namespace nu {

class SpinLock {
public:
  SpinLock();
  SpinLock(const SpinLock &) = delete;
  SpinLock &operator=(const SpinLock &) = delete;
  ~SpinLock();
  void lock();
  void unlock();
  bool try_lock();

private:
  spinlock_t spinlock_;
  friend class CondVar;
};
} // namespace nu

#include "nu/impl/spinlock.ipp"
