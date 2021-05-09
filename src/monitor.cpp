extern "C" {
#include <runtime/timer.h>
}
#include "thread.h"

#include "migrator.hpp"
#include "monitor.hpp"
#include "runtime.hpp"

namespace nu {

// TODO: use real signals.

Monitor::Monitor()
  : mock_pressure_({.cores = 0, .mem_mbs = 0}), stopped_(false) {}

Monitor::~Monitor() {
  stopped_ = true;
  continuous_ = false;
}

void Monitor::run_loop() {
  while (!ACCESS_ONCE(stopped_)) {
    timer_sleep(kPollIntervalUs);
    auto pressure = detect_pressure();
    if (!pressure.empty()) {
      auto heaps = Runtime::heap_manager->pick_heaps(pressure);
      if (!heaps.empty()) {
        Runtime::migrator->migrate(pressure, heaps);
      }
      if (!continuous_) {
        Resource empty = {.cores = 0, .mem_mbs = 0};
        mock_set_pressure(empty);
      }
    }
  }
}

void Monitor::mock_set_pressure(Resource pressure) {
  mock_pressure_ = pressure;
}

void Monitor::mock_set_continuous() { continuous_ = true; }

void Monitor::mock_clear_continuous() { continuous_ = false; }

Resource Monitor::detect_pressure() { return mock_pressure_; }

} // namespace nu
