#pragma once

#include "sync.h"

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
  rt::Spin spinlock_;
};
} // namespace nu

#include "impl/spinlock.ipp"
