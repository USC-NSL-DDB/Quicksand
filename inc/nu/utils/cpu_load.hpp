#pragma once

#include "nu/commons.hpp"

namespace nu {

class CPULoad {
 public:
  constexpr static uint32_t kSampleInterval = 32;  // Be power of 2 for speed.
  constexpr static uint32_t kDecayIntervalUs = 100 * kOneMilliSecond;
  constexpr static float kEMWAWeight = 0.25;

  CPULoad();
  void start_monitor();
  void end_monitor();
  float get_load() const;
  static void flush_all();

 private:
  aligned_cycles cycles_[kNumCores];
  struct alignas(kCacheLineBytes) {
    uint64_t invocations;
    uint64_t samples;
  } cnts_[kNumCores];
  uint64_t last_decay_tsc_;
  float cpu_load_;

  void decay(uint64_t now_tsc);
};

}  // namespace nu

#include "nu/impl/cpu_load.ipp"
