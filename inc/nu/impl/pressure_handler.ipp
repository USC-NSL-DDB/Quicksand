#include <runtime.h>

namespace nu {

inline bool PressureHandler::has_pressure() {
  return rt::RuntimeToReleaseMemMbs() || rt::RuntimeCpuPressure() || mock_;
}

inline void PressureHandler::start_aux_handlers() {
  for (auto &state : aux_handler_states_) {
    state.done = false;
  }
}

inline void PressureHandler::stop_aux_handlers() {
  for (auto &state : aux_handler_states_) {
    state.done = true;
  }
}

}  // namespace nu
