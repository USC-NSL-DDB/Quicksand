#include <sync.h>

#include "nu/commons.hpp"
#include "nu/utils/cpu_load.hpp"
#include "nu/runtime.hpp"

namespace nu {

void CPULoad::decay(uint64_t now_tsc) {
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
          : 1.0f;
  auto cycles_ratio =
      static_cast<float>(sum_cycles) / (now_tsc - last_decay_tsc_);
  auto latest_cpu_load = sample_ratio_inverse * cycles_ratio;

  memset(cycles_, 0, sizeof(cycles_));
  memset(cnts_, 0, sizeof(cnts_));
  last_decay_tsc_ = now_tsc;

  ewma(kEMWAWeight, &cpu_load_, latest_cpu_load);
}

}  // namespace nu
