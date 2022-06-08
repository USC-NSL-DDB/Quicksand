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

inline CPULoad::State CPULoad::monitor_start() {
  State state;
  auto core_id = read_cpu();

  if (likely((cnts_[core_id].invocations++ % kSampleInterval) &&
             !thread_monitored())) {
    state.sampled = false;
    state.caller_output = nullptr;
  } else {
    state.sampled = true;
    cnts_[core_id].samples++;
    state.caller_output = thread_start_monitor_cycles(cycles_);
  }
  return state;
}

inline void CPULoad::monitor_end(const State &state) {
  if (likely(!state.sampled)) {
    return;
  }
  thread_end_monitor_cycles(state.caller_output);
}

inline void CPULoad::flush_all() { thread_flush_all_monitor_cycles(); }

}  // namespace nu
