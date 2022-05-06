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

#include "nu/pressure_handler.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"

using namespace nu;

constexpr static int kConcurrency = 100;

Runtime::Mode mode;

namespace nu {
class Test {
public:
  int get_credits() { return credits_; }

  void consume() {
    mutex_.lock();
    while (ACCESS_ONCE(credits_) == 0) {
      condvar_.wait(&mutex_);
    }
    credits_--;
    mutex_.unlock();
  }

  void produce() {
    mutex_.lock();
    credits_++;
    condvar_.signal();
    mutex_.unlock();
  }

  void migrate() {
    ResourcePressureInfo pressure = {.mem_mbs_to_release = 1000,
                                     .num_cores_to_release = 0};
    Runtime::pressure_handler->mock_set_pressure(pressure);
  }

private:
  CondVar condvar_;
  Mutex mutex_;
  int credits_ = 0;
};
} // namespace nu

void do_work() {
  auto proclet = make_proclet<Test>();

  std::vector<Future<void>> futures;
  for (size_t i = 0; i < kConcurrency; i++) {
    futures.push_back(std::move(proclet.run_async(&Test::consume)));
  }

  futures.push_back(std::move(proclet.run_async(&Test::migrate)));

  for (size_t i = 0; i < kConcurrency; i++) {
    futures.push_back(std::move(proclet.run_async(&Test::produce)));
  }

  for (auto &future : futures) {
    future.get();
  }

  bool passed = (proclet.run(&Test::get_credits) == 0);

  if (passed) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
