#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include <sync.h>
#include <thread.h>

#include "defs.hpp"

namespace nu {

class TraceLogger {
public:
  constexpr static uint32_t kNumBuckets = 11;
  constexpr static uint32_t kBucketIntervalUs = 50;

  TraceLogger();
  ~TraceLogger();
  void enable_print(uint32_t interval_us);
  void disable_print();
  template <typename F> std::pair<uint64_t, uint64_t> add_trace(F &&f);

private:
  struct alignas(kCacheLineBytes) AlignedCnt {
    uint64_t cnt;
  };

  AlignedCnt aligned_cnts_[kNumCores][kNumBuckets];
  uint32_t print_interval_us_;
  rt::Thread print_thread_;
  bool disabled_;
  rt::Mutex mutex_;
  rt::CondVar cv_;
  bool done_;

  void check_disabled();
};

} // namespace nu

#include "impl/trace_logger.ipp"
