#pragma once

#include <sync.h>

namespace nu {

class SpinLock {
public:
  SpinLock();
  SpinLock(const SpinLock &) = delete;
  SpinLock &operator=(const SpinLock &) = delete;
  ~SpinLock();
  void Lock();
  void Unlock();
  bool TryLock();

private:
  spinlock_t spinlock_;
};
} // namespace nu

#include "nu/impl/spinlock.ipp"
