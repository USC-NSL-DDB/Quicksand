#include <algorithm>
#include <functional>
#include <iostream>
#include <ranges>
#include <type_traits>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_stack.hpp"
#include "utils.hpp"

constexpr uint32_t kRunTimes = 1;
constexpr uint32_t kNumElems = 2'000'000;

void bench_push_pop_ints() {
  {
    // TODO: bench with batching once the stack implementation supports it
    auto s = nu::make_sharded_stack<int, std::true_type>();

    auto insertion_time = time([&] {
      for (int i : std::views::iota(0, static_cast<int>(kNumElems))) {
        s.push(i);
      }
      for (int i = 0; i < static_cast<int>(kNumElems); ++i) {
        s.pop();
      }
      s.flush();
    });

    std::cout << "\t\tShardedStack:\t" << insertion_time << " us" << std::endl;
  }

  {
    nu::RuntimeSlabGuard slab;
    std::stack<int> s;

    auto insertion_time = time([&] {
      for (int i : std::views::iota(0, static_cast<int>(kNumElems))) {
        s.push(i);
      }
      for (int i = 0; i < static_cast<int>(kNumElems); ++i) {
        s.pop();
      }
    });

    std::cout << "\t\tstd::stack:\t" << insertion_time << " us" << std::endl;
  }
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv,
                               [](int, char **) { bench_push_pop_ints(); });
}
