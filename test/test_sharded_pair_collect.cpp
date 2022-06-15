#include <algorithm>
#include <cereal/types/string.hpp>
#include <functional>
#include <iostream>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_pair_collect.hpp"

constexpr uint32_t kNumElements = 1 << 20;

bool run_test() {
  nu::ShardedPairCollection<int, std::string> sc;
  std::function emplace_thread_fn = [&](uint32_t start, uint32_t end) {
    return nu::Thread([&, start, end] {
      for (uint32_t i = start; i < end; i++) {
        auto str = std::to_string(i);
        sc.emplace_back(i, str);
      }
    });
  };
  auto t0 = emplace_thread_fn(0, kNumElements / 2);
  auto t1 = emplace_thread_fn(kNumElements / 2, kNumElements);
  t0.join();
  t1.join();

  std::function for_all_thread_fn = [&] {
    return nu::Thread([&] {
      sc.for_all(
          +[](std::pair<const int, std::string> &p, char new_c) {
            p.second += new_c;
          },
          ' ');
    });
  };
  auto t2 = for_all_thread_fn();
  auto t3 = for_all_thread_fn();
  t2.join();
  t3.join();

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
