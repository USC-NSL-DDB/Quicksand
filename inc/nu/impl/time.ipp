extern "C" {
#include <runtime/timer.h>
}

#include "nu/heap_mgr.hpp"
#include "nu/runtime.hpp"

namespace nu {

inline Time::Time() : offset_tsc_(0) {}

inline void Time::delay(uint64_t us) { delay_us(us); }

inline uint64_t Time::to_logical_tsc(uint64_t physical_tsc) {
  return physical_tsc + offset_tsc_;
}

inline uint64_t Time::to_logical_us(uint64_t physical_us) {
  return physical_us + offset_tsc_ / cycles_per_us;
}

inline uint64_t Time::to_physical_tsc(uint64_t logical_tsc) {
  return logical_tsc - offset_tsc_;
}

inline uint64_t Time::to_physical_us(uint64_t logical_us) {
  return logical_us - offset_tsc_ / cycles_per_us;
}

inline uint64_t Time::obj_env_microtime() {
  return to_logical_us(::microtime());
}

inline void Time::obj_env_sleep(uint64_t duration_us) {
  obj_env_sleep_until(obj_env_microtime() + duration_us);
}

} // namespace nu
