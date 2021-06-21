#pragma once

#include <cstdint>
#include <optional>

#include "defs.hpp"

namespace nu {

class Monitor {
public:
  constexpr static uint32_t kPollIntervalUs = 1000;
  constexpr static double kMemLowWaterMark = 0.05;

  Monitor();
  void run_loop();
  void stop_loop();
  void mock_set_pressure(Resource pressure);

private:
  std::optional<Resource> mock_pressure_;
  bool stopped_;

  bool detect_pressure(Resource *pressure);
};
} // namespace nu
