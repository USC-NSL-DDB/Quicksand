#pragma once

#include <cstdint>
#include <functional>

#include <sync.h>

#include "defs.hpp"

namespace nu {

class RCULock {
public:
  void reader_lock();
  bool reader_lock_np();
  void reader_unlock();
  void reader_unlock_np();
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
  bool detect_sync_barrier_np();
};
} // namespace nu

