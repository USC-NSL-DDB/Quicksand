#include <algorithm>
#include <cereal/types/string.hpp>
#include <functional>
#include <iostream>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_pair_collect.hpp"

constexpr uint32_t kNumElements = 1 << 20;

class Worker {
 public:
  Worker(nu::ShardedPairCollection<int, std::string> sc) : sc_(sc) {}

  void emplace(uint32_t start, uint32_t end) {
    for (uint32_t i = start; i < end; i++) {
      auto str = std::to_string(i);
      sc_.emplace_back(i, str);
    }
    sc_.flush();
  }

  void mutate() {
    sc_.for_all(
        +[](std::pair<const int, std::string> &p, char new_c) {
          p.second += new_c;
        },
        ' ');
  }

 private:
  nu::ShardedPairCollection<int, std::string> sc_;
};

bool run_test() {
  nu::ShardedPairCollection<int, std::string> sc;
  auto p0 = make_proclet<Worker>(sc);
  auto p1 = make_proclet<Worker>(sc);

  auto f0 = p0.run_async(&Worker::emplace, 0, kNumElements / 4);
  auto f1 = p0.run_async(&Worker::emplace, kNumElements / 4, kNumElements / 2);
  auto f2 = p1.run_async(&Worker::emplace, kNumElements / 2, kNumElements / 4 * 3);
  auto f3 = p1.run_async(&Worker::emplace, kNumElements / 4 * 3, kNumElements);
  f0.get();
  f1.get();
  f2.get();
  f3.get();

  auto f4 = p0.run_async(&Worker::mutate);
  auto f5 = p1.run_async(&Worker::mutate);
  f4.get();
  f5.get();

  auto v = sc.collect();
  sort(v.begin(), v.end());

  std::vector<std::pair<int, std::string>> expected_v;
  for (uint32_t i = 0; i < kNumElements; i++) {
    expected_v.emplace_back(i, std::string(std::to_string(i) + "  "));
  }

  return v == expected_v;
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    if (run_test()) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });
}
