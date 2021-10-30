#include <sync.h>

#include "nu/utils/cpu_load.hpp"

namespace nu {

void CPULoad::reset() {
  last_refresh_tsc = 0;
  memset(infos, 0, sizeof(infos));
}

void CPULoad::add_trace(uint64_t start_tsc, uint64_t end_tsc) {
  rt::Preempt p;
  rt::PreemptGuard guard(&p);

  assert(start_tsc < end_tsc);
  auto last_tsc = last_refresh_tsc;
  if (unlikely(last_tsc + kRefreshIntervalTSC < start_tsc)) {
    if (__sync_bool_compare_and_swap(&last_refresh_tsc, last_tsc, start_tsc)) {
      for (uint32_t i = 0; i < kNumCores; i++) {
        infos[i].refresh = true;
      }
    }
  }

  auto &info = infos[p.get_cpu()];
  if (unlikely(info.refresh)) {
    info.active_tsc = 0;
    info.refresh = false;
  }

  info.active_tsc += end_tsc - start_tsc;
}

double CPULoad::get_load() const {
  uint64_t active_sum = 0;

  auto curr_tsc = rdtsc();
  for (uint32_t i = 0; i < kNumCores; i++) {
    auto &info = infos[i];
    if (!info.refresh) {
      active_sum += info.active_tsc;
    }
  }

  return static_cast<double>(active_sum) / (curr_tsc - last_refresh_tsc);
}
} // namespace nu
