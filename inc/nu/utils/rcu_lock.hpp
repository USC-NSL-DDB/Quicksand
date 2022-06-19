#pragma once

#include <sync.h>

#include <cstdint>
#include <functional>

#include "nu/commons.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/spin_lock.hpp"

namespace nu {

class RCULock {
 public:
  constexpr static uint32_t kReaderWaitFastPathMaxUs = 20;
  constexpr static uint32_t kWriterWaitFastPathMaxUs = 20;
  constexpr static uint32_t kWriterWaitSlowPathSleepUs = 10;

  RCULock();
  ~RCULock();
  void reader_lock();
  bool try_reader_lock();  
  void reader_unlock();
  void reader_lock_np();
  bool try_reader_lock_np();
  void reader_unlock_np();
  void writer_sync(bool prioritize_readers = false);

 private:
  union Cnt {
    struct Data {
      int32_t c;
      int32_t ver;
    } data;
    uint64_t raw;
  };
  static_assert(sizeof(Cnt) == sizeof(Cnt::Data));

  struct alignas(kCacheLineBytes) AlignedCnt {
    Cnt cnt;
  };

  bool sync_barrier_;
  AlignedCnt aligned_cnts_[kNumCores];
  std::vector<rt::ThreadWaker> wakers_;
  rt::Spin spin_;

  template <typename Fn>
  void write_sync_general(Fn &&fn);
  void reader_wait();
  void __reader_lock_np();
  void __reader_lock();
};
}  // namespace nu

#include "nu/impl/rcu_lock.ipp"
