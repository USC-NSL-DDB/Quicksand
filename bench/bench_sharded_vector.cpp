#include <algorithm>
#include <cereal/types/string.hpp>
#include <functional>
#include <iostream>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_vector.hpp"

constexpr uint32_t kNumElements = 10 << 25;
constexpr uint32_t kRunTimes = 1;
constexpr uint32_t kPowerShardSize = 20;

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
      auto t0 = microtime();
      for (uint32_t i = 0; i < kNumElements; i++) {
        vec.push_back(1);
      }
      auto t1 = microtime();
      std::cout << "\t\tShardedVector:\t" << t1 - t0 << " us" << std::endl;

      auto t2 = microtime();
      size_t x = 0;
      for (uint32_t i = 0; i < kNumElements; i++) {
        x += vec[i];
      }
      auto t3 = microtime();
      std::cout << "\t\t ---- Result: " << x << std::endl;
      std::cout << "\t\tShardedVector sequential access:\t" << t3 - t2 << " us"
                << std::endl;

      auto t4 = microtime();
      size_t sum = vec.reduce(
          0, +[](int sum, int x) { return sum + x; });
      auto t5 = microtime();
      std::cout << "\t\t ---- Result: " << sum << std::endl;
      std::cout << "\t\tShardedVector reduction access:\t" << t5 - t4 << " us"
                << std::endl;
    }

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
      std::cout << "\t\t ---- Result: " << x << std::endl;
      std::cout << "\t\tstd::vector sequential access:\t" << t3 - t2 << " us"
                << std::endl;
    }
  }
};

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv,
                               [](int, char **) { nu::make_proclet<Work>(); });
}
