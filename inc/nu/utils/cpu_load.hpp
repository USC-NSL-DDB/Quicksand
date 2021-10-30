#pragma once

#include "nu/commons.hpp"

namespace nu {

class CPULoad {
public:
  constexpr static uint32_t kSampleInterval = 32; // Be power of 2 for speed.
  constexpr static uint64_t kRefreshIntervalTSC = 5ULL * 2800 * 1000 * 1000;

  struct State {
    bool sampled;
    uint64_t start_tsc;
  };

  void reset();
  State monitor_start();
  void monitor_end(const State &state);
  double get_load() const;

private:
  uint64_t last_refresh_tsc;
  struct {
    bool refresh;
    uint64_t active_tsc;
    uint64_t sample_cnt;
  } infos[kNumCores];

  State __monitor_start();
  void __monitor_end(const State &state);
};

} // namespace nu

#include "nu/impl/cpu_load.ipp"
