#pragma once

#include <cstdint>
#include <functional>

#include <sync.h>

#include "nu/commons.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"

namespace nu {

class RCULock {
public:
  RCULock();
  ~RCULock();
  void reader_lock();
  bool try_reader_lock();
  void reader_unlock();
  void writer_sync();

private:
  union Cnt {
    struct Data {
      int32_t c;
      int32_t ver;
    } data;
    uint64_t raw;
  };
  static_assert(sizeof(Cnt) == sizeof(Cnt::Data));

  struct alignas(kCacheLineBytes) PerCoreData {
    Cnt cnt;
    rt::Spin spin;
    std::vector<rt::ThreadWaker> wakers;
  };

  bool sync_barrier_;
  PerCoreData per_core_data_[kNumCores];

  template <typename Fn> void write_sync_general(Fn &&fn);
  void detect_sync_barrier();
  void __detect_sync_barrier();
};
} // namespace nu

#include "nu/impl/rcu_lock.ipp"
