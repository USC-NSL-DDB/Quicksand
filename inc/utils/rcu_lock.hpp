#pragma once

#include <cstdint>

#include "defs.hpp"

namespace nu {

class RCULock {
public:
  void lock();
  void unlock();
  void synchronize();

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
};
} // namespace nu

#include "impl/rcu_lock.ipp"
