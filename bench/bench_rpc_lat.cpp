#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}

#include "rem_obj.hpp"
#include "runtime.hpp"
#include "utils/bench.hpp"

using namespace nu;
using namespace std;

constexpr static uint32_t kNumRuns = 100000;
Runtime::Mode mode;

class Obj {
public:
  int foo() { return 0x88; }

private:
};

void do_work() {
  auto rem_obj = RemObj<Obj>::create();

  std::vector<uint64_t> tscs;
  for (uint32_t i = 0; i < kNumRuns; i++) {
    auto start_tsc = rdtsc();
    auto ret = rem_obj.run(&Obj::foo);
    auto end_tsc = rdtsc();
    tscs.push_back(end_tsc - start_tsc);
    BUG_ON(ret != 0x88);
  }
  print_percentile(&tscs);
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
