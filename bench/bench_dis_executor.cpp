#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <type_traits>

#include "nu/cont_ds_range.hpp"
#include "nu/dis_executor.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_queue.hpp"
#include "nu/sharded_vector.hpp"
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

void bench_rate_match_with_synthetic_input() {
  std::cout << __FUNCTION__ << std::endl;

  constexpr uint32_t kNormalDelayUs = 1000;
  constexpr uint32_t kBurstDelayUs = 100;
  constexpr int kInsertElem = 123;

  auto queue = nu::make_sharded_queue<int, std::true_type>();

  auto t0 = microtime();
  auto t_scale_up = t0 + nu::kOneSecond * 10;
  auto t_scale_down = t0 + nu::kOneSecond * 15;

  auto producers = queue.produce(
      +[](uint64_t t_scale_up, uint64_t t_scale_down) {
        auto curr = microtime();
        if (curr >= t_scale_up && curr < t_scale_down) {
          compute_us<kBurstDelayUs>();
        } else {
          compute_us<kNormalDelayUs>();
        }
        return kInsertElem;
      },
      t_scale_up, t_scale_down);

  auto consumers =
      queue.consume(+[](int elem) { compute_us<kNormalDelayUs>(); });

  consumers.get();
  producers.get();
}

template <typename T, typename LL, typename ProcessFn, typename ConsumeFn>
void bench_rate_match(nu::ShardedVector<T, LL> input, ProcessFn process_fn,
                      ConsumeFn consume_fn) {
  auto input_rng =
      nu::make_contiguous_ds_range(nu::to_sealed_ds(std::move(input)));
  auto queue = nu::make_sharded_queue<T, std::true_type>();

  auto producers = nu::make_distributed_executor(
      +[](decltype(input_rng) &input_rng, decltype(queue) queue,
          decltype(process_fn) process_fn) {
        while (!input_rng.empty()) {
          auto elem = input_rng.pop();
          auto processed = process_fn(std::move(elem));
          queue.push(std::move(processed));
        }
      },
      input_rng, queue, process_fn);

  auto consumers = queue.consume(consume_fn);

  consumers.get();
  producers.get();
}

void bench_rate_match_with_int_vec() {
  constexpr std::size_t kNumElems = 1 << 10;
  constexpr auto kProcessFn = [](std::size_t elem) {
    compute_us<1000>();
    return elem;
  };
  constexpr auto kConsumeFn = [](std::size_t elem) { compute_us<1000>(); };

  auto input = nu::make_sharded_vector<std::size_t, std::false_type>();
  for (auto i : std::views::iota(static_cast<std::size_t>(0), kNumElems)) {
    input.push_back(i);
  }

  bench_rate_match(input, +kProcessFn, +kConsumeFn);
}

void do_work() {
  bench_vector_task_range();
  // bench_rate_match();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
