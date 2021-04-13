#pragma once

#include <cstdint>
#include <functional>

#include "defs.hpp"

namespace nu {

class RCULock {
public:
  void reader_lock();
  void reader_unlock();
  void writer_sync();
  void writer_sync_fn(const std::function<void(void)> &fn);
  void writer_sync_fn(std::function<void(void)> &&fn);

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
};
} // namespace nu

#include "impl/rcu_lock.ipp"
