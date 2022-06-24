#include <algorithm>
#include <cereal/types/string.hpp>
#include <functional>
#include <iostream>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_vector.hpp"

constexpr uint32_t kNumElements = 10 << 30;
constexpr uint32_t kRunTimes = 1;
constexpr uint32_t kPowerShardSize = 20;

template <typename F>
inline uint64_t time(F fn) {
  auto t0 = microtime();
  fn();
  auto t1 = microtime();
  return t1 - t0;
}

class Work {
 public:
  Work() {
    std::cout << "Num elements: " << kNumElements << std::endl;
    for (uint32_t i = 0; i < kRunTimes; i++) {
      std::cout << "Running No." << i << " time..." << std::endl;
      single_thread();
      // TODO: add more.
    }
  }

  void single_thread() {
    std::cout << "\tRunning single-thread bench..." << std::endl;
    {
      auto vec = nu::make_sharded_vector<int>(kPowerShardSize);

      auto insertion_time = time([&]() {
        for (uint32_t i = 0; i < kNumElements; i++) {
          vec.push_back(1);
        }
      });
      std::cout << "\t\tShardedVector:\t" << insertion_time << " us"
                << std::endl;

      size_t x = 0;
      auto naive_summation_time = time([&]() {
        for (uint32_t i = 0; i < kNumElements; i++) {
          x += vec[i];
        }
      });
      std::cout << "\t\t ---- sum: " << x << std::endl;
      std::cout << "\t\tShardedVector sequential access:"
                << naive_summation_time << " us" << std::endl;

      size_t sum;
      auto reduced_summation_time = time([&]() {
        sum = vec.reduce(
            0, +[](int sum, int x) { return sum + x; });
      });
      std::cout << "\t\t ---- sum: " << sum << std::endl;
      std::cout << "\t\tShardedVector reduction access:\t"
                << reduced_summation_time << " us" << std::endl;

      auto for_all_time =
          time([&]() { vec.for_all(+[](int x) { return 0; }); });
      std::cout << "\t\tShardedVector for_all access:\t" << for_all_time
                << " us" << std::endl;
    }

    std::cout << std::endl;

    {
      nu::RuntimeSlabGuard slab;
      std::vector<int> v;
      auto t0 = microtime();
      for (uint32_t i = 0; i < kNumElements; i++) {
        v.push_back(1);
      }
      auto t1 = microtime();
      std::cout << "\t\tstd::vector:\t" << t1 - t0 << " us" << std::endl;

      auto t2 = microtime();
      size_t x = 0;
      for (uint32_t i = 0; i < kNumElements; i++) {
        x += v[i];
      }
      auto t3 = microtime();
      std::cout << "\t\t ---- sum: " << x << std::endl;
      std::cout << "\t\tstd::vector sequential access:\t" << t3 - t2 << " us"
                << std::endl;
    }
  }
};

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv,
                               [](int, char **) { nu::make_proclet<Work>(); });
}
