#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>

#include "nu/dis_executor.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_queue.hpp"
#include "nu/utils/time.hpp"
#include "nu/vector_task_range.hpp"

constexpr uint32_t kDelayUs = nu::kOneMilliSecond;

template <uint32_t Micros>
void compute_us() {
  const auto num_iters = Micros * cycles_per_us;
  for (uint32_t i = 0; i < num_iters; i++) {
    asm volatile("nop");
  }
}

void bench_vector_task_range() {
  std::cout << "bench_vector_task_range" << std::endl;

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
          compute_us<kDelayUs>();
        }
        return outputs;
      },
      nu::VectorTaskRange<std::size_t>(inputs));

  dis_exec.get();

  barrier();
  auto t1 = microtime();
  barrier();

  std::cout << "\t" << t1 - t0 << std::endl;
}

void bench_rate_match() {
  std::cout << "bench_rate_match" << std::endl;

  constexpr uint32_t kNormalDelayUs = 1000;
  constexpr uint32_t kBurstDelayUs = 100;

  auto queue = nu::make_sharded_queue<int, std::true_type>();

  auto produce_rng = nu::make_writeable_queue_range(queue);
  auto consume_rng = nu::make_queue_range(queue);

  auto t0 = microtime();
  auto t_scale_up = t0 + nu::kOneSecond * 3;
  auto t_scale_down = t0 + nu::kOneSecond * 6;

  auto producers = nu::make_distributed_executor(
      +[](decltype(produce_rng) &rng, uint64_t t_scale_up,
          uint64_t t_scale_down) {
        while (true) {
          auto inserter = rng.pop();
          inserter = 33;

          auto curr = microtime();
          if (curr >= t_scale_up && curr <= t_scale_down) {
            compute_us<kBurstDelayUs>();
          } else {
            compute_us<kNormalDelayUs>();
          }
        }
      },
      produce_rng, t_scale_up, t_scale_down);

  auto consumers = nu::make_distributed_executor(
      +[](decltype(consume_rng) &rng) {
        while (true) {
          rng.pop();
          compute_us<kNormalDelayUs>();
        }
      },
      consume_rng);

  consumers.get();
  producers.get();
}

void do_work() {
  bench_vector_task_range();
  // bench_rate_match();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
