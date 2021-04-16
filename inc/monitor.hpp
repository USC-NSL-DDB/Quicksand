#pragma once

#include <cstdint>

#include "defs.hpp"

namespace nu {

class Monitor {
public:
  constexpr static uint32_t kPollIntervalUs = 100;

  Monitor();
  ~Monitor();
  void run_loop();
  void mock_set_pressure(Resource pressure);

private:
  Resource mock_pressure_;
  bool stopped_;

  Resource detect_pressure();
};
} // namespace nu
