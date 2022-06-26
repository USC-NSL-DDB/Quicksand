#include <algorithm>
#include <cereal/types/string.hpp>
#include <functional>
#include <iostream>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_pair_collect.hpp"

constexpr uint32_t kRunTimes = 1;
constexpr uint32_t kNumElements = 160 << 20;
constexpr uint32_t kNumThreads = 32;

class Work {
 public:
  Work() {
    for (uint32_t i = 0; i < kRunTimes; i++) {
      std::cout << "Running No." << i << " time..." << std::endl;
      single_thread();
      multi_threads();
      // TODO: add more.
    }
  }

  void single_thread() {
    std::cout << "\tRunning single-thread bench..." << std::endl;
    {
      auto sc = nu::make_sharded_pair_collection<int, int>();
      auto t0 = microtime();
      for (uint32_t i = 0; i < kNumElements; i++) {
        sc.emplace(i, i);
      }
      auto t1 = microtime();
      std::cout << "\t\tShardedPairCollection: "
                << static_cast<double>(kNumElements) / (t1 - t0) << " MOPS"
                << std::endl;
    }

    {
      nu::RuntimeSlabGuard slab;
      std::vector<std::pair<int, int>> v;
      auto t0 = microtime();
      for (uint32_t i = 0; i < kNumElements; i++) {
        v.emplace_back(i, i);
      }
      auto t1 = microtime();
      std::cout << "\t\tstd::vector: "
                << static_cast<double>(kNumElements) / (t1 - t0) << " MOPS"
                << std::endl;
    }
  }

  void multi_threads() {
    std::cout << "\tRunning multi-thread bench..." << std::endl;

    auto sc = nu::make_sharded_pair_collection<int, int>(
        kNumElements, 0, [](int &x, uint64_t offset) { x += offset; });
    std::vector<nu::Thread> ths;

    for (uint32_t i = 0; i < kNumThreads; i++) {
      ths.emplace_back([&sc, tid = i] {
        auto num_elems_per_th = kNumElements / kNumThreads;
        for (uint32_t i = 0; i < num_elems_per_th; i++) {
          sc.emplace(tid * num_elems_per_th + i, i);
        }
      });
    }
    auto t0 = microtime();
    for (auto &th : ths) {
      th.join();
    }
    auto t1 = microtime();
    std::cout << "\t\tShardedPairCollection: " << kNumElements / (t1 - t0)
              << " MOPS" << std::endl;
  }
};

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv,
                               [](int, char **) { nu::make_proclet<Work>(); });
}
