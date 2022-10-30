#include <cstring>

extern "C" {
#include <runtime/thread.h>
}

namespace nu {

inline CPULoad::CPULoad() { reset(); }

inline void CPULoad::reset() {
  last_refresh_tsc_ = rdtsc();
  memset(cycles_, 0, sizeof(cycles_));
  memset(cnts_, 0, sizeof(cnts_));
}

inline void CPULoad::start_monitor() {
  auto core_id = read_cpu();

  if (unlikely(cnts_[core_id].invocations++ % kSampleInterval == 0 ||
               thread_monitored())) {
    cnts_[core_id].samples++;
    thread_start_monitor_cycles();
  }
}

inline void CPULoad::end_monitor() { thread_end_monitor_cycles(); }

inline void CPULoad::flush_all() { thread_flush_all_monitor_cycles(); }

}  // namespace nu
