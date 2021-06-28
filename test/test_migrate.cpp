#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

extern "C" {
#include <base/time.h>
#include <net/ip.h>
}
#include <runtime.h>

#include "nu/monitor.hpp"
#include "nu/rem_obj.hpp"
#include "nu/runtime.hpp"

using namespace nu;

constexpr static int kMagic = 0xDEADBEEF;

Runtime::Mode mode;

namespace nu {
class Test {
public:
  int run() {
    // Should be printed at the initial server node.
    std::cout << "I am here" << std::endl;
    Resource resource = {.cores = 0, .mem_mbs = 1000};
    // Mock a resource pressure which will be detected by the nu::Monitor
    // instance very quickly.
    Runtime::monitor->mock_set_pressure(resource);
    // Ensure that the migration happens before the function returns.
    delay_us(1000 * 1000);
    // Should be printed at the new server node.
    std::cout << "I am here" << std::endl;
    return kMagic;
  }
};
} // namespace nu

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) {
    auto rem_obj = RemObj<Test>::create();
    bool passed = (rem_obj.run(&Test::run) == kMagic);

    if (passed) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });
}
