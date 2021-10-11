#pragma once

#include <cstdint>
#include <optional>

#include "nu/commons.hpp"

namespace nu {

class Monitor {
public:
  constexpr static bool kMonitorCPUCongestion = false;
  constexpr static uint32_t kPollIntervalUs = 1000;
  constexpr static double kMemLowWaterMarkMB = 1024; // 1 GB.

  Monitor();
  void run_loop();
  void stop_loop();
  void mock_set_pressure(Resource pressure);

private:
  std::optional<Resource> mock_pressure_;
  bool stopped_;
  uint32_t past_granted_cores_;

  bool detect_pressure(Resource *pressure);
};
} // namespace nu
