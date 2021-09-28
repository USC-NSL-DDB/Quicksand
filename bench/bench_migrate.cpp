#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

extern "C" {
#include <base/time.h>
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "nu/monitor.hpp"
#include "nu/rem_obj.hpp"
#include "nu/runtime.hpp"

using namespace nu;

Runtime::Mode mode;

constexpr uint32_t kObjSize = 128 << 10;
constexpr uint32_t kNumRuns = 5;

namespace nu {
class Test {
public:
  void run() {
    Resource resource = {.cores = 0, .mem_mbs = 1000};
    Runtime::monitor->mock_set_pressure(resource);
    delay_ms(1000);
  }

private:
  uint8_t heap[kObjSize];
};
} // namespace nu

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) {
    for (uint32_t k = 0; k < kNumRuns; k++) {
      auto rem_obj = RemObj<Test>::create();
      rem_obj.run(&Test::run);
      delay_ms(100);
    }
  });
}
