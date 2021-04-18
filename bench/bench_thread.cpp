#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/net.h>
#include <runtime/runtime.h>
#include <runtime/tcp.h>
#include <runtime/timer.h>
}
#include <thread.h>

#include "defs.hpp"
#include "utils/bench.hpp"
#include "utils/tcp.hpp"

using namespace nu;
using namespace std;

constexpr static uint32_t kNumThreads = 8;

void _main(void *args) {
  std::vector<rt::Thread> threads;
  threads.reserve(kNumThreads);

  auto start = rdtsc();
  uint64_t starts_tid[kNumThreads];
  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, tid = i] {
      starts_tid[tid] = rdtsc();
      delay_ms(1);
    });
  }

  for (auto &thread : threads) {
    thread.Join();
  }

  for (uint32_t i = 0; i < kNumThreads; i++) {
    std::cout << starts_tid[i] - start << std::endl;
  }
}

int main(int argc, char **argv) {
  int ret;

  if (argc < 2) {
    goto wrong_args;
  }
  ret = runtime_init(argv[1], _main, NULL);
  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }
  return 0;

wrong_args:
  std::cerr << "usage: [cfg_file]" << std::endl;
  return -EINVAL;
}
