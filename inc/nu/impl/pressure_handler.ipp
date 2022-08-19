#include <runtime.h>

namespace nu {

inline bool PressureHandler::has_pressure() {
  return rt::RuntimeToReleaseMemMbs() || rt::RuntimeCpuPressure() || mock_;
}

}  // namespace nu
