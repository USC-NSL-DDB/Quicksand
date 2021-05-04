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

#include "monitor.hpp"
#include "rem_obj.hpp"
#include "runtime.hpp"

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

  ret = rt::RuntimeInit(std::string(argv[1]), [] {
    std::cout << "Running " << __FILE__ "..." << std::endl;
    bool passed = true;

    netaddr remote_ctrl_addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 3), .port = 8000};
    auto runtime = Runtime::init(/* local_obj_srv_port = */ 8001,
                                 /* local_migrator_port = */ 8002,
                                 /* remote_ctrl_addr = */ remote_ctrl_addr,
                                 /* mode = */ mode);

    auto rem_obj = RemObj<Test>::create();
    passed = (rem_obj.run(&Test::run) == kMagic);

    if (passed) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;

wrong_args:
  std::cerr << "usage: [cfg_file] CLT/SRV/CTL" << std::endl;
  return -EINVAL;
}
