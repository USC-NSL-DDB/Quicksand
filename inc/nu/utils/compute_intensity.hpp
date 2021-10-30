#pragma once

#include "nu/commons.hpp"

namespace nu {

class ComputeIntensity {
public:
  constexpr static uint64_t kRefreshIntervalTSC = 5ULL * 2800 * 1000 * 1000;

  void reset();
  void add_trace(uint64_t start_tsc, uint64_t end_tsc);
  double get_compute_intensity() const;

private:
  uint64_t last_refresh_tsc;
  struct {
    bool refresh;
    uint64_t active_tsc;
  } infos[kNumCores];
};

} // namespace nu
