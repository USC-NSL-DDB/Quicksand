#include <sync.h>

#include "nu/runtime.hpp"
#include "nu/utils/cpu_load.hpp"

namespace nu {

double CPULoad::get_load() const {
  uint64_t sum_cycles = 0;
  uint64_t sum_invocation_cnts = 0;
  uint64_t sum_sample_cnts = 0;

  for (uint32_t i = 0; i < kNumCores; i++) {
    sum_cycles += cycles_[i].c;
    sum_invocation_cnts += cnts_[i].invocations;
    sum_sample_cnts += cnts_[i].samples;
  }

  return static_cast<double>(sum_invocation_cnts) / sum_sample_cnts *
         sum_cycles / (rdtsc() - last_refresh_tsc_);
}

} // namespace nu
