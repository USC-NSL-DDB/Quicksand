#include <sync.h>

#include "nu/runtime.hpp"
#include "nu/utils/cpu_load.hpp"

namespace nu {

float CPULoad::get_load() const {
  uint64_t sum_cycles = 0;
  uint64_t sum_invocation_cnts = 0;
  uint64_t sum_sample_cnts = 0;

  for (uint32_t i = 0; i < kNumCores; i++) {
    sum_cycles += cycles_[i].c;
    sum_invocation_cnts += cnts_[i].invocations;
    sum_sample_cnts += cnts_[i].samples;
  }
  auto sample_ratio_inverse =
      sum_sample_cnts
          ? static_cast<float>(sum_invocation_cnts) / sum_sample_cnts
          : 1.0;
  auto cycles_ratio =
      static_cast<float>(sum_cycles) / (rdtsc() - last_refresh_tsc_);
  return sample_ratio_inverse * cycles_ratio;
}

} // namespace nu
