#include <algorithm>
#include <cereal/types/string.hpp>
#include <functional>
#include <iostream>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_pair_collect.hpp"

constexpr uint32_t kNumElements = 10 << 20;
constexpr uint32_t kRunTimes = 1;

class Work {
 public:
  Work() {
    for (uint32_t i = 0; i < kRunTimes; i++) {
      std::cout << "Running No." << i << " time..." << std::endl;
      single_thread();
      // TODO: add more.
    }
  }

  void single_thread() {
    std::cout << "\tRunning single-thread bench..." << std::endl;
    {
      nu::ShardedPairCollection<int, int> sc;
      auto t0 = microtime();
      for (uint32_t i = 0; i < kNumElements; i++) {
        sc.emplace(i, i);
      }
      auto t1 = microtime();
      std::cout << "\t\tShardedPairCollection: " << t1 - t0 << " us"
                << std::endl;
    }

    {
      std::vector<std::pair<int, int>> v;
      auto t0 = microtime();
      for (uint32_t i = 0; i < kNumElements; i++) {
        v.emplace_back(i, i);
      }
      auto t1 = microtime();
      std::cout << "\t\tstd::vector: " << t1 - t0 << " us" << std::endl;
    }
  }
};

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv,
                               [](int, char **) { nu::make_proclet<Work>(); });
}
