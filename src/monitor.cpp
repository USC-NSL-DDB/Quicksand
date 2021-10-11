#include <iostream>
#include <sys/sysinfo.h>

extern "C" {
#include <runtime/timer.h>
}
#include <runtime.h>
#include <thread.h>

#include "nu/commons.hpp"
#include "nu/migrator.hpp"
#include "nu/monitor.hpp"
#include "nu/runtime.hpp"

constexpr static bool kEnableLogging = false;

namespace nu {

Monitor::Monitor()
    : mock_pressure_(std::nullopt), stopped_(false),
      past_granted_cores_(rt::RuntimeMaxCores()) // For now we just assume all
                                                 // cores are granted initially
{}

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

      auto core_ratio = static_cast<double>(pressure.cores) /
                        (pressure.cores + past_granted_cores_);
      auto min_num_heaps =
          Runtime::heap_manager->get_num_present_heaps() * core_ratio;
      auto min_mem_mbs = pressure.mem_mbs;
      auto heaps =
          Runtime::heap_manager->pick_heaps(min_num_heaps, min_mem_mbs);
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
    // Detect memory pressure through Linux interface.
    struct sysinfo info;
    BUG_ON(sysinfo(&info) != 0);
    auto free_ram_mbs = info.freeram / kOneMB;
    if (free_ram_mbs < kMemLowWaterMarkMB) {
      pressure->mem_mbs = kMemLowWaterMarkMB - free_ram_mbs;
      has_pressure = true;
    }

    // Detect cpu pressure through Caladan interface.
    if (kMonitorCPUCongestion && rt::RuntimeCongested()) {
      auto granted_cores = rt::RuntimeGrantedCores();
      if (granted_cores < past_granted_cores_) {
        pressure->cores = past_granted_cores_ - granted_cores;
        past_granted_cores_ = granted_cores;
        has_pressure = true;
      }
    }
  }
  return has_pressure;
}

} // namespace nu
