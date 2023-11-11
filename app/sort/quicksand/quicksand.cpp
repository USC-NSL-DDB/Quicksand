#include <algorithm>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <ranges>
#include <utility>

#include "nu/cereal.hpp"
#include "nu/dis_executor.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_sorter.hpp"

using Key = uint64_t;
constexpr uint64_t kNumElements = 400ULL << 20;
constexpr uint32_t kValSize = 90;
constexpr auto kNormalDistributionMean = std::numeric_limits<Key>::max() / 2.0;
constexpr auto kNormalDistributionStdDev = kNumElements / 10;
constexpr auto kUniformDistributionMin = 0;
constexpr auto kUniformDistributionMax = std::numeric_limits<Key>::max();
constexpr bool kUseNormalDistribution = true;

struct Val {
  char data[kValSize];

  Val() = default;
  Val(char x) { memset(data, x, sizeof(data)); }

  bool operator<(const Val &o) const {
    return std::strncmp(data, o.data, sizeof(data)) < 0;
  }
};

void do_work() {
  auto sharded_sorter = nu::make_sharded_sorter<Key, Val>();

  constexpr uint64_t kBatchSize = 4 << 20;
  auto range = std::views::repeat(kBatchSize, kNumElements / kBatchSize);
  std::vector<int> tasks(range.begin(), range.end());
  if (kNumElements % kBatchSize) {
    tasks.emplace_back(kNumElements % kBatchSize);
  }

  barrier();
  auto t0 = microtime();
  barrier();

  {
    auto dis_exec = nu::make_distributed_executor(
        +[](nu::VectorTaskRange<int> &task_range,
            decltype(sharded_sorter) sharded_sorter) {
          std::mt19937 gen{0};
          std::normal_distribution<double> normal_d{kNormalDistributionMean,
                                                    kNormalDistributionStdDev};
          std::uniform_int_distribution<uint64_t> uniform_d{
              kUniformDistributionMin, kUniformDistributionMax};

          while (auto task = task_range.pop()) {
	    auto upper = *task;
            for (int i = 0; i < upper; i++) {
              auto key =
                  kUseNormalDistribution ? normal_d(gen) : uniform_d(gen);
              auto val = Val(i);
              sharded_sorter.insert(key, val);
            }
          }
        },
        nu::VectorTaskRange<int>(tasks), sharded_sorter);
    dis_exec.get();
  }

  barrier();
  auto t1 = microtime();
  barrier();

  auto sharded_sorted = sharded_sorter.sort();

  barrier();
  auto t2 = microtime();
  barrier();

  std::cout << "Shuffling takes " << t1 - t0 << " microseconds." << std::endl;
  std::cout << "Sorting takes " << t2 - t1 << " microseconds." << std::endl;
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
