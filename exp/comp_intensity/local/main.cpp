#include <iostream>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_vector.hpp"

constexpr uint64_t kTotalMemUsage = 15ULL * 1024 * 1024 * 1024;
constexpr uint64_t kNumComputeThreads = 26;
constexpr uint64_t kElementSize = 100;
constexpr uint64_t kNumElements =
    kTotalMemUsage / kNumComputeThreads / kElementSize;
constexpr uint64_t kDelayUs = 10;

struct Element {
  Element() = default;
  uint64_t data[kElementSize / sizeof(uint64_t)];
};

void compute_on(const Element &e) {
  for (const auto &d : e.data) {
    ACCESS_ONCE(d);
  }
  {
    nu::Caladan::PreemptGuard g;

    unsigned long start = rdtsc();
    auto delay_cycles = kDelayUs * cycles_per_us;
    while (rdtsc() - start < delay_cycles) {
      cpu_relax();
    }
  }
}

void run() {
  std::vector<Element> std_vecs[kNumComputeThreads];

  {
    std::vector<nu::Thread> threads;
    for (uint64_t i = 0; i < kNumComputeThreads; i++) {
      threads.emplace_back(nu::Thread([std_vec_ptr = &std_vecs[i]] {
        for (uint64_t i = 0; i < kNumElements; i++) {
          std_vec_ptr->push_back(Element());
        }
      }));
    }
    for (auto &thread : threads) {
      thread.join();
    }
  }

  barrier();
  auto t0 = microtime();
  barrier();

  {
    std::vector<nu::Thread> threads;
    for (uint64_t i = 0; i < kNumComputeThreads; i++) {
      threads.emplace_back(nu::Thread([std_vec_ptr = &std_vecs[i]] {
        for (const auto &element : *std_vec_ptr) {
          compute_on(element);
        }
      }));
    }
    for (auto &thread : threads) {
      thread.join();
    }
  }

  barrier();
  auto t1 = microtime();
  barrier();

  std::cout << t1 - t0 << std::endl;
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [&](int, char **) { run(); });
}
