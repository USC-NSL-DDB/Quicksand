#include <algorithm>
#include <iostream>
#include <ranges>
#include <utility>

#include "nu/runtime.hpp"
#include "nu/sharded_sorter.hpp"

constexpr uint64_t kNumElements = 107957637;
using Key = uint64_t;
using Val = uint64_t;

void run_sharded_sorter() {
  std::cout << "run_sharded_sorter()..." << std::endl;

  auto sharded_sorter = nu::make_sharded_sorter<Key, Val>();

  barrier();
  auto t0 = microtime();
  barrier();

  for (Key i : std::views::iota(0ULL, kNumElements) | std::views::reverse) {
    sharded_sorter.insert(i, Val());
  }

  barrier();
  auto t1 = microtime();
  barrier();

  auto sharded_sorted = sharded_sorter.sort();

  barrier();
  auto t2 = microtime();
  barrier();

  std::cout << "\ttime: " << t1 - t0 << " " << t2 - t1 << " " << std::endl;
}

void run_std_sorter() {
  std::cout << "run_std_sorter()..." << std::endl;

  std::vector<std::pair<Key, Val>> vec;

  barrier();
  auto t0 = microtime();
  barrier();

  for (Key i : std::views::iota(static_cast<Key>(0), kNumElements) |
                   std::views::reverse) {
    vec.emplace_back(i, Val());
  }

  barrier();
  auto t1 = microtime();
  barrier();

  std::ranges::sort(vec);

  barrier();
  auto t2 = microtime();
  barrier();

  std::cout << "\ttime: " << t1 - t0 << " " << t2 - t1 << " " << std::endl;  
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    run_std_sorter();
    run_sharded_sorter();
  });
}
