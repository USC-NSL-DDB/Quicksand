#include <runtime.h>

namespace nu {

inline bool PressureHandler::has_pressure() {
  return rt::RuntimeToReleaseMemMbs() || rt::RuntimeCpuPressure();
}

inline bool PressureHandler::has_real_pressure() {
  return has_pressure() && !mock_;
}

}  // namespace nu
