#pragma once

#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/rcu_lock.hpp"

namespace nu {

class ReaderWriterLock {
 public:
  constexpr static uint32_t kReaderWaitFastPathMaxUs = 20;

  ReaderWriterLock();
  void reader_lock();
  void reader_unlock();
  void writer_lock();
  void writer_unlock();

 private:
  bool writer_barrier_;
  Mutex mutex_;
  RCULock rcu_lock_;
  CondVar cond_var_;
};

}  // namespace nu

#include "nu/impl/reader_writer_lock.ipp"
