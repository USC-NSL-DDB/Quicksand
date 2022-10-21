#include <iostream>
#include <ranges>

#include "nu/runtime.hpp"
#include "nu/sharded_sorter.hpp"

constexpr int kNumElements = 16 << 20;

bool run_test() {
  auto sharded_sorter = nu::make_sharded_sorter<int, int>();

  for (int i : std::views::iota(0, kNumElements) | std::views::reverse) {
    sharded_sorter.emplace(i, i);
  }
  auto sharded_sorted = sharded_sorter.sort();

  int idx = 0;
  for (const auto &[k, v] : sharded_sorted) {
    if (k != idx || v != idx) {
      return false;
    }
    idx++;
  }

  return idx == kNumElements;
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
