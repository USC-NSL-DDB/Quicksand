#include <algorithm>
#include <cereal/types/string.hpp>
#include <functional>
#include <iostream>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_pair_collect.hpp"

constexpr uint32_t kRunTimes = 1;
constexpr uint32_t kNumElements = 160 << 20;
constexpr uint32_t kNumThreads = 4 * (nu::kNumCores - 2);

class Work {
 public:
  Work() {
    for (uint32_t i = 0; i < kRunTimes; i++) {
      std::cout << "Running No." << i << " time..." << std::endl;
      single_thread_std_vector();
      single_thread_no_partition();
      single_thread_perfect_partition();
      multi_threads_no_partition();
      multi_threads_perfect_partition();
      // TODO: add more.
    }
  }

  void single_thread_std_vector() {
    std::cout << "\tRunning single-thread-std-vector ench..." << std::endl;

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

  void single_thread_no_partition() {
    std::cout << "\tRunning single-thread-no-partition bench..." << std::endl;
    auto sc = nu::make_sharded_pair_collection<int, int>();
    single_thread(&sc);
  }

  void single_thread_perfect_partition() {
    std::cout << "\tRunning single-thread-perfect-partition bench..."
              << std::endl;
    auto sc = nu::make_sharded_pair_collection<int, int>(
        kNumElements, 0, [](int &x, uint64_t offset) { x += offset; });
    single_thread(&sc);
  }

  void single_thread(nu::ShardedPairCollection<int, int> *sc) {
    auto t0 = microtime();
    for (uint32_t i = 0; i < kNumElements; i++) {
      sc->emplace(i, i);
    }
    auto t1 = microtime();
    std::cout << "\t\tShardedPairCollection: "
              << static_cast<double>(kNumElements) / (t1 - t0) << " MOPS"
              << std::endl;
  }

  void multi_threads_no_partition() {
    std::cout << "\tRunning multi-threads-no-partition bench..." << std::endl;
    auto sc = nu::make_sharded_pair_collection<int, int>();
    multi_threads(&sc);
  }

  void multi_threads_perfect_partition() {
    std::cout << "\tRunning multi-threads-perfect-partition bench..."
              << std::endl;
    auto sc = nu::make_sharded_pair_collection<int, int>(
        kNumElements, 0, [](int &x, uint64_t offset) { x += offset; });
    multi_threads(&sc);
  }

  void multi_threads(nu::ShardedPairCollection<int, int> *sc) {
    std::vector<nu::Thread> ths;
    for (uint32_t i = 0; i < kNumThreads; i++) {
      ths.emplace_back([sc, tid = i] {
        auto num_elems_per_th = kNumElements / kNumThreads;
        for (uint32_t i = 0; i < num_elems_per_th; i++) {
          sc->emplace(tid * num_elems_per_th + i, i);
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
