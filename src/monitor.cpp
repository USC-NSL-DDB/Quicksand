#include <iostream>
#include <sys/sysinfo.h>

extern "C" {
#include <runtime/timer.h>
}
#include <thread.h>

#include "nu/defs.hpp"
#include "nu/migrator.hpp"
#include "nu/monitor.hpp"
#include "nu/runtime.hpp"

constexpr static bool kEnableLogging = false;

namespace nu {

Monitor::Monitor() : mock_pressure_(std::nullopt), stopped_(false) {}

void Monitor::stop_loop() { ACCESS_ONCE(stopped_) = true; }

void Monitor::run_loop() {
  auto last_poll_us = microtime();
  while (!ACCESS_ONCE(stopped_)) {
    timer_sleep(last_poll_us + kPollIntervalUs - microtime());

    Resource pressure;
    auto has_pressure = detect_pressure(&pressure);

    last_poll_us = microtime();

    if (has_pressure) {
      if constexpr (kEnableLogging) {
        std::cout << "Detect pressure = { .cores = " << pressure.cores
                  << ", .mem_mbs = " << pressure.mem_mbs << " }." << std::endl;
      }

      auto heaps = Runtime::heap_manager->pick_heaps(pressure);
      if (!heaps.empty()) {
        Runtime::migrator->migrate(pressure, heaps);
      }
    }
  }
}

void Monitor::mock_set_pressure(Resource pressure) {
  mock_pressure_ = pressure;
}

bool Monitor::detect_pressure(Resource *pressure) {
  bool has_pressure = false;
  __builtin_memset(pressure, 0, sizeof(*pressure));

  if (mock_pressure_) {
    // From the mock interface.
    *pressure = *mock_pressure_;
    mock_pressure_ = std::nullopt;
    has_pressure = true;
  } else {
    // Detect the real pressure through Linux interface.
    struct sysinfo info;
    BUG_ON(sysinfo(&info) != 0);
    auto expected_freeram = info.totalram * kMemLowWaterMark;
    if (info.freeram < expected_freeram) {
      pressure->mem_mbs = (expected_freeram - info.freeram) / kOneMB + 1;
      has_pressure = true;
    }
  }
  return has_pressure;
}

} // namespace nu
