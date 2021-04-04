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
#include "mutex.hpp"
#include "utils/spinlock.hpp"

using namespace nu;

constexpr static int kConcurrency = 100;

Runtime::Mode mode;

namespace nu {
class Test {
public:

  void do_work() { delay_us(100 * 1000); }

  void mutex() {
    mutex_.lock();
    do_work();
    cnt_++;
    mutex_.unlock();
  }

  void migrate() {
    Resource resource = {.cores = 0, .mem_mbs = 1000};
    Runtime::monitor->mock_set_pressure(resource);
  }

  int get_cnt() { return cnt_; }

private:
  Mutex mutex_;
  int cnt_ = 0;
};
} // namespace nu

void _main(void *args) {
  std::cout << "Running " << __FILE__ "..." << std::endl;
  bool passed = true;

  netaddr remote_ctrl_addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 3), .port = 8000};
  auto runtime = Runtime::init(/* local_obj_srv_port = */ 8001,
                               /* local_migra_ldr_port = */ 8002,
                               /* remote_ctrl_addr = */ remote_ctrl_addr,
                               /* mode = */ mode);

  auto rem_obj = RemObj<Test>::create();

  std::vector<Future<void>> futures;
  for (size_t i = 0; i < kConcurrency; i++) {
    futures.push_back(std::move(rem_obj.run_async(&Test::mutex)));
  }
  futures.push_back(std::move(rem_obj.run_async(&Test::migrate)));

  for (auto &future : futures) {
    future.get();
  }

  passed &= (rem_obj.run(&Test::get_cnt) == kConcurrency);

  if (passed) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
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
