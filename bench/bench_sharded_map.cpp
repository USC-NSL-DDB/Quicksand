#include <algorithm>
#include <cereal/types/string.hpp>
#include <functional>
#include <iostream>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_map.hpp"
#include "utils.hpp"

constexpr uint32_t kRunTimes = 1;

void bench_insert_ints() {
  int num_pairs = 2'000'000;
  {
    auto m = nu::make_sharded_map<int, int, std::false_type>();

    auto insertion_time = time([&] {
      for (int i = 0; i < num_pairs; i++) {
        m.emplace(i, i);
      }
      m.flush();
    });

    std::cout << "\t\tShardedMap:\t" << insertion_time << " us" << std::endl;
  }

  {
    nu::RuntimeSlabGuard slab;
    std::map<int, int> m;

    auto insertion_time = time([&] {
      for (int i = 0; i < num_pairs; i++) {
        m[i] = i;
      }
    });

    std::cout << "\t\tstd::map:\t" << insertion_time << " us" << std::endl;
  }
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv,
                               [](int, char **) { bench_insert_ints(); });
}
