#include <algorithm>
#include <functional>
#include <iostream>
#include <ranges>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_set.hpp"
#include "utils.hpp"

constexpr uint32_t kRunTimes = 1;
constexpr uint32_t kNumElems = 2'000'000;

void bench_insert_ints() {
  {
    auto ms = nu::make_sharded_multi_set<int, std::false_type>();

    auto insertion_time = time([&] {
      for (int i : std::views::iota(0, static_cast<int>(kNumElems))) {
        ms.insert(i);
      }
      ms.flush();
    });

    std::cout << "\t\tShardedMultiSet:\t" << insertion_time << " us"
              << std::endl;
  }

  {
    nu::RuntimeSlabGuard slab;
    std::multiset<int> ms;

    auto insertion_time = time([&] {
      for (int i : std::views::iota(0, static_cast<int>(kNumElems))) {
        ms.insert(i);
      }
    });

    std::cout << "\t\tstd::multi_set:\t" << insertion_time << " us"
              << std::endl;
  }
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv,
                               [](int, char **) { bench_insert_ints(); });
}
