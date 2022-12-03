#include <cstring>

#include "nu/runtime.hpp"

namespace nu {

inline CPULoad::CPULoad() {
  memset(cycles_, 0, sizeof(cycles_));
  memset(cnts_, 0, sizeof(cnts_));
  last_decay_tsc_ = rdtsc();
  cpu_load_ = 0;
}

inline void CPULoad::start_monitor() {
  auto core_id = read_cpu();

  if (unlikely(cnts_[core_id].invocations++ % kSampleInterval == 0 ||
               get_runtime()->caladan()->thread_monitored())) {
    cnts_[core_id].samples++;
    get_runtime()->caladan()->thread_start_monitor_cycles();
  }
}

inline void CPULoad::end_monitor() {
  get_runtime()->caladan()->thread_end_monitor_cycles();
}

inline void CPULoad::flush_all() {
  get_runtime()->caladan()->thread_flush_all_monitor_cycles();
}

inline float CPULoad::get_load() const {
  auto now_tsc = rdtsc();
  if (unlikely(now_tsc >= last_decay_tsc_ + kDecayIntervalUs * cycles_per_us)) {
    const_cast<CPULoad *>(this)->decay(now_tsc);
  }

  return cpu_load_;
}

}  // namespace nu
