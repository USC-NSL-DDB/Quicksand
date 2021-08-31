#pragma once

#include <cstdint>
#include <functional>

#include "nu/commons.hpp"

namespace nu {

class RCULock {
public:
  RCULock();
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

  struct alignas(kCacheLineBytes) AlignedCnt {
    Cnt cnt;
  };

  bool sync_barrier_;
  AlignedCnt aligned_cnts_[kNumCores];

  template <typename Fn> void write_sync_general(Fn &&fn);
  void detect_sync_barrier();
};
} // namespace nu

#include "nu/impl/rcu_lock.ipp"

