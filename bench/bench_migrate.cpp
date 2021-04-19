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

#include "monitor.hpp"
#include "rem_obj.hpp"
#include "runtime.hpp"

using namespace nu;

Runtime::Mode mode;

constexpr uint32_t kHeapSize = (128 << 10) - kPtrHeaderSize;
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
  uint8_t heap[kHeapSize];
};
} // namespace nu

void _main(void *args) {
  std::cout << "Running " << __FILE__ "..." << std::endl;

  netaddr remote_ctrl_addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 3), .port = 8000};
  auto runtime = Runtime::init(/* local_obj_srv_port = */ 8001,
                               /* local_migrator_port = */ 8002,
                               /* remote_ctrl_addr = */ remote_ctrl_addr,
                               /* mode = */ mode);

  for (uint32_t k = 0; k < kNumRuns; k++) {
    auto rem_obj = RemObj<Test>::create();
    rem_obj.run(&Test::run);
    delay_ms(100);
  }
}

int main(int argc, char **argv) {
  int ret;
  std::string mode_str;

  if (argc < 3) {
    goto wrong_args;
  }

  mode_str = std::string(argv[2]);
  if (mode_str == "CLT") {
    mode = Runtime::Mode::CLIENT;
  } else if (mode_str == "SRV") {
    mode = Runtime::Mode::SERVER;
  } else if (mode_str == "CTL") {
    mode = Runtime::Mode::CONTROLLER;
  } else {
    goto wrong_args;
  }

  ret = runtime_init(argv[1], _main, NULL);
  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;

wrong_args:
  std::cerr << "usage: [cfg_file] CLT/SRV/CTL" << std::endl;
  return -EINVAL;
}
