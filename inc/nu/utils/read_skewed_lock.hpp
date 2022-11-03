#pragma once

#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/rcu_lock.hpp"

namespace nu {

class ReadSkewedLock {
 public:
  constexpr static uint32_t kReaderWaitFastPathMaxUs = 20;

  ReadSkewedLock();
  void reader_lock();
  void reader_unlock();
  void reader_lock_np();
  void reader_unlock_np();
  void writer_lock();
  void writer_unlock();

 private:
  bool writer_barrier_;
  Mutex mutex_;
  RCULock rcu_lock_;
  CondVar cond_var_;

  void reader_wait(bool np);
};

}  // namespace nu

#include "nu/impl/read_skewed_lock.ipp"
