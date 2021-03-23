extern "C" {
#include <runtime/timer.h>
}
#include "thread.h"

#include "migrator.hpp"
#include "monitor.hpp"
#include "runtime.hpp"

namespace nu {

Monitor::Monitor() : stopped_(false) {}

Monitor::~Monitor() { stopped_ = true; }

void Monitor::run_loop_async() {
  rt::Thread([&] {
    while (ACCESS_ONCE(stopped_)) {
      timer_sleep(kPollIntervalUs);
      auto pressure = detect_pressure();
      auto heaps = Runtime::heap_manager->pick_heaps(pressure);
      Runtime::migrator->migrate(heaps);
    }
  });
}

void Monitor::mock_set_pressure(Pressure pressure) {
  mock_pressure_ = pressure;
}

Pressure Monitor::detect_pressure() { return mock_pressure_; }

} // namespace nu
