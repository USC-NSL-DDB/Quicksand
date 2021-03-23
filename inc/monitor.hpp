#pragma once

#include <cstdint>

namespace nu {

struct Pressure {
  // TODO: add cpu pressure.
  uint32_t mem_mbs;
};

class Monitor {
public:
  constexpr static uint32_t kPollIntervalUs = 2;

  Monitor();
  ~Monitor();
  void run_loop_async();
  void mock_set_pressure(Pressure pressure);

private:
  Pressure mock_pressure_;
  bool stopped_;

  Pressure detect_pressure();
};
} // namespace nu
