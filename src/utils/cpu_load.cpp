#include <sync.h>

#include "nu/runtime.hpp"
#include "nu/utils/cpu_load.hpp"

namespace nu {

CPULoad::State CPULoad::__monitor_start() {
  State state;
  state.sampled = true;
  state.start_tsc = rdtsc();
  state.start_cycles = get_thread_running_cycles(state.start_tsc);
  return state;
}

void CPULoad::__monitor_end(const State &state) {
  rt::Preempt p;
  rt::PreemptGuard guard(&p);

  auto start_tsc = state.start_tsc;
  auto last_tsc = last_refresh_tsc;
  auto start_cycles = state.start_cycles;
  auto now_tsc = rdtsc();
  auto end_cycles = get_thread_running_cycles(now_tsc);
  assert(start_cycles < end_cycles);
  assert(start_tsc < now_tsc);

  if (unlikely(last_tsc + kRefreshIntervalTSC < start_tsc)) {
    if (__sync_bool_compare_and_swap(&last_refresh_tsc, last_tsc, start_tsc)) {
      for (uint32_t i = 0; i < kNumCores; i++) {
        infos[i].refresh = true;
      }
    }
  }

  auto &info = infos[p.get_cpu()];
  if (unlikely(info.refresh)) {
    info.active_cycles = 0;
    info.refresh = false;
  }

  info.active_cycles += end_cycles - start_cycles;
}

double CPULoad::get_load() const {
  uint64_t active_sum = 0;

  for (uint32_t i = 0; i < kNumCores; i++) {
    auto &info = infos[i];
    if (!info.refresh) {
      active_sum += info.active_cycles;
    }
  }

  return static_cast<double>(kSampleInterval) * active_sum /
         (rdtsc() - last_refresh_tsc);
}
} // namespace nu
