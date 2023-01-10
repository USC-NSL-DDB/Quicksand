#include <algorithm>
#include <cstring>
#include <functional>
#include <iostream>

#include "nu/dis_executor.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/time.hpp"
#include "nu/vector_task_range.hpp"

constexpr uint32_t kDelayUs = nu::kOneMilliSecond;

void compute() {
  const auto num_iters = kDelayUs * cycles_per_us;
  for (uint32_t i = 0; i < num_iters; i++) {
    asm volatile("nop");
  }
}

void do_work() {
  constexpr std::size_t kSize = (1 << 20);

  std::vector<std::size_t> inputs;
  for (auto i : std::views::iota(static_cast<std::size_t>(0), kSize)) {
    inputs.push_back(i);
  }

  barrier();
  auto t0 = microtime();
  barrier();

  auto dis_exec = nu::make_distributed_executor(
      +[](nu::VectorTaskRange<std::size_t> &task_range) {
        std::vector<std::size_t> outputs;
        while (!task_range.empty()) {
          task_range.pop();
          compute();
        }
        return outputs;
      },
      nu::VectorTaskRange<std::size_t>(inputs));

  dis_exec.get();

  barrier();
  auto t1 = microtime();
  barrier();

  std::cout << t1 - t0 << std::endl;
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
