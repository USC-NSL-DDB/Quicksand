#pragma once

#include "nu/commons.hpp"

namespace nu {

class CPULoad {
public:
  constexpr static uint32_t kSampleInterval = 32; // Be power of 2 for speed.

  struct State {
    bool sampled;
    struct aligned_cycles *caller_output;
  };

  CPULoad();
  void reset();
  State monitor_start();
  void monitor_end(const State &state);
  float get_load() const;
  static void flush_all();

private:
  uint64_t last_refresh_tsc_;
  aligned_cycles cycles_[kNumCores];
  struct alignas(kCacheLineBytes) {
    uint64_t invocations;
    uint64_t samples;
  } cnts_[kNumCores];
};

} // namespace nu

#include "nu/impl/cpu_load.ipp"
