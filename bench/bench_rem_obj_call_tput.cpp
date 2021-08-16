#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "nu/rem_obj.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/bench.hpp"

using namespace nu;
using namespace std;

constexpr static uint32_t kNumThreads = 300;
Runtime::Mode mode;

struct AlignedCnt {
  uint32_t cnt;
  uint8_t pads[kCacheLineBytes - sizeof(cnt)];
};

AlignedCnt cnts[kNumThreads];

class Obj {
public:
  int foo() { return 0x88; }

private:
};

void do_work() {
  auto rem_obj = RemObj<Obj>::create();

  for (uint32_t i = 0; i < kNumThreads; i++) {
    rt::Thread([&, tid = i] {
      while (true) {
        auto ret = rem_obj.run(&Obj::foo);
        ACCESS_ONCE(ret);
        cnts[tid].cnt++;
      }
    }).Detach();
  }

  uint64_t old_sum = 0;
  uint64_t old_us = microtime();
  while (true) {
    timer_sleep(1000 * 1000);
    auto us = microtime();
    uint64_t sum = 0;
    for (uint32_t i = 0; i < kNumThreads; i++) {
      sum += ACCESS_ONCE(cnts[i].cnt);
    }
    std::cout << us - old_us << " " << sum - old_sum << std::endl;
    old_sum = sum;
    old_us = us;
  }
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
