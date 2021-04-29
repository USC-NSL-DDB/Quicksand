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

#include "cond_var.hpp"
#include "monitor.hpp"
#include "mutex.hpp"
#include "rem_obj.hpp"
#include "runtime.hpp"

using namespace nu;

constexpr static int kConcurrency = 100;

Runtime::Mode mode;

namespace nu {
class Test {
public:
  int get_credits() {
    return credits_;
  }

  void consume() {
    mutex_.Lock();
    while (ACCESS_ONCE(credits_) == 0) {
      condvar_.wait(&mutex_);
    }
    credits_--;
    mutex_.Unlock();
  }

  void produce() {
    mutex_.Lock();
    credits_++;
    condvar_.signal();
    mutex_.Unlock();
  }

  void migrate() {
    Resource resource = {.cores = 0, .mem_mbs = 1000};
    Runtime::monitor->mock_set_pressure(resource);
  }

private:
  CondVar condvar_;
  Mutex mutex_;
  int credits_ = 0;
};
} // namespace nu

void _main(void *args) {
  std::cout << "Running " << __FILE__ "..." << std::endl;
  bool passed;
  netaddr remote_ctrl_addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 3), .port = 8000};
  auto runtime = Runtime::init(/* local_obj_srv_port = */ 8001,
                               /* local_migrator_port = */ 8002,
                               /* remote_ctrl_addr = */ remote_ctrl_addr,
                               /* mode = */ mode);

  auto rem_obj = RemObj<Test>::create();

  std::vector<Future<void>> futures;
  for (size_t i = 0; i < kConcurrency; i++) {
    futures.push_back(std::move(rem_obj.run_async(&Test::consume)));
  }

  futures.push_back(std::move(rem_obj.run_async(&Test::migrate)));

  for (size_t i = 0; i < kConcurrency; i++) {
    futures.push_back(std::move(rem_obj.run_async(&Test::produce)));
  }

  for (auto &future : futures) {
    future.get();
  }

  passed = (rem_obj.run(&Test::get_credits) == 0);

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
