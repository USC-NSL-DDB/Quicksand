#include <algorithm>
#include <cstdint>
#include <iostream>
#include <vector>
#include <cmath>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}

#include "monitor.hpp"
#include "rem_obj.hpp"
#include "runtime.hpp"
#include "time.hpp"

using namespace nu;

Runtime::Mode mode;

namespace nu {

class Test {
public:
  uint64_t microtime() { return Time::microtime(); }
  void delay(uint64_t us) { return Time::delay(us); }
  void sleep_until(uint64_t deadline_us) {
    return Time::sleep_until(deadline_us);
  }
  void sleep(uint64_t duration_us) { return Time::sleep(duration_us); }
  void migrate() {
    Resource resource = {.cores = 0, .mem_mbs = 1000};
    Runtime::monitor->mock_set_pressure(resource);
  }
};
} // namespace nu

bool around_one_second(uint64_t us) {
  std::cout << us << std::endl;
  return abs(static_cast<int64_t>(us) - 1000 * 1000) < 5000;
}

void do_work() {
  bool passed = true;
  uint64_t us[5];

  auto rem_obj = RemObj<Test>::create();
  us[0] = rem_obj.run(&Test::microtime);
  rem_obj.run(&Test::delay, 1000 * 1000);
  us[1] = rem_obj.run(&Test::microtime);
  rem_obj.run(&Test::sleep, 1000 * 1000);
  us[2] = rem_obj.run(&Test::microtime);
  rem_obj.run(+[](Test &t) {
    auto us = t.microtime();
    t.sleep_until(us + 1000 * 1000);
  });
  us[3] = rem_obj.run(&Test::microtime);
  auto future = rem_obj.run_async(&Test::sleep, 1000 * 1000);
  rem_obj.run(&Test::migrate);
  future.get();
  us[4] = rem_obj.run(&Test::microtime);

  for (uint64_t i = 0; i < 4; i++) {
    passed &= around_one_second(us[i + 1] - us[i]);
  }

  if (passed) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

void _main(void *args) {
  std::cout << "Running " << __FILE__ "..." << std::endl;

  netaddr remote_ctrl_addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 3), .port = 8000};
  auto runtime = Runtime::init(/* local_obj_srv_port = */ 8001,
                               /* local_migra_ldr_port = */ 8002,
                               /* remote_ctrl_addr = */ remote_ctrl_addr,
                               /* mode = */ mode);
  do_work();
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
