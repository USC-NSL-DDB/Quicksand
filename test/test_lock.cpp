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
#include "mutex.hpp"
#include "rem_obj.hpp"
#include "runtime.hpp"
#include "utils/spinlock.hpp"

using namespace nu;

constexpr static int kConcurrency = 100;

Runtime::Mode mode;

namespace nu {
class Test {
public:
  void do_work() { delay_us(100 * 1000); }

  void mutex() {
    mutex_.Lock();
    do_work();
    cnt_++;
    mutex_.Unlock();
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

void do_work() {
  bool passed = true;

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
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
