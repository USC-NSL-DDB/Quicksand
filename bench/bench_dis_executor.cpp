#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <type_traits>

#include "nu/commons.hpp"
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

enum GPUStatus {
  kRunning = 0,
  kDrain,
  kTerminate,
};

using GPUStatusType = uint8_t;

template <typename Item>
class MockGPU {
 public:
  static constexpr uint32_t kProcessDelayUs = 3500;

  MockGPU() {}
  void run(nu::ShardedQueue<Item, std::true_type> queue) {
    constexpr std::size_t kPopNumItems = 10;

    while (true) {
      auto status = rt::access_once(status_);
      if (unlikely(status == GPUStatus::kTerminate)) {
        break;
      }
      auto popped = queue.try_pop(kPopNumItems);
      if (unlikely(status == GPUStatus::kDrain && popped.size() == 0)) {
        break;
      }
      for (auto &p : popped) {
        process(std::move(p));
      }
    }
  }
  void drain_and_stop() { status_ = GPUStatus::kDrain; }
  void stop() { status_ = GPUStatus::kTerminate; }

 private:
  void process(Item &&item) { compute_us<kProcessDelayUs>(); }

  GPUStatusType status_ = GPUStatus::kRunning;
};

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
  auto sealed_input = nu::to_sealed_ds(std::move(input));
  auto input_rng = nu::make_contiguous_ds_range(sealed_input);
  auto queue = nu::make_sharded_queue<T, std::true_type>();

  auto producers = nu::make_distributed_executor(
      +[](decltype(input_rng) &input_rng, decltype(queue) queue,
          decltype(process_fn) process_fn) {
        while (true) {
          auto elem = input_rng.pop();
          if (unlikely(!elem)) {
            break;
          }
          auto processed = process_fn(std::move(*elem));
          queue.push(std::move(processed));
        }
      },
      input_rng, queue, process_fn);

  auto consumers = queue.consume(consume_fn);

  producers.get();
  consumers.drain_and_join();
}

void bench_rate_match_with_int_vec() {
  std::cout << __FUNCTION__ << std::endl;

  constexpr uint64_t kConsumeTime = 4000;
  constexpr uint64_t kProcessTime = 3500;

  constexpr std::size_t kNumElems = 1 << 12;
  constexpr auto kProcessFn = [](std::size_t elem) {
    compute_us<kProcessTime>();
    return elem;
  };
  constexpr auto kConsumeFn = [](std::size_t elem) {
    compute_us<kConsumeTime>();
  };

  auto input = nu::make_sharded_vector<std::size_t, std::false_type>();
  for (auto i : std::views::iota(static_cast<std::size_t>(0), kNumElems)) {
    input.push_back(i);
  }

  bench_rate_match(std::move(input), +kProcessFn, +kConsumeFn);
}

void bench_temp_gpu_slot() {
  std::cout << __FUNCTION__ << std::endl;

  using Elem = std::vector<char>;
  using GPU = MockGPU<Elem>;

  constexpr uint64_t kProcessTime = 4000;
  constexpr uint64_t kNumGPUs = 4;
  constexpr uint64_t kNumTempGPUs = 4;
  constexpr uint64_t kScaleUpDurationUs = nu::kOneMilliSecond * 10;
  constexpr uint64_t kNumScaleUps = 20;
  constexpr std::size_t kElemSize = 150'000;
  constexpr std::size_t kNumElems = 1 << 12;

  Elem elem;
  elem.reserve(kElemSize);
  for (std::size_t j = 0; j < kElemSize; ++j) {
    elem.push_back(1);
  }

  auto input = nu::make_sharded_vector<Elem, std::false_type>();
  for (std::size_t i = 0; i < kNumElems; ++i) {
    input.push_back(elem);
  }

  auto sealed_input = nu::to_sealed_ds(std::move(input));
  auto input_rng = nu::make_contiguous_ds_range(sealed_input);
  auto queue = nu::make_sharded_queue<Elem, std::true_type>();

  auto gpus = std::vector<nu::Proclet<GPU>>{};
  auto futures = std::vector<nu::Future<void>>{};
  for (uint64_t i = 0; i < kNumGPUs; ++i) {
    gpus.emplace_back(nu::make_proclet<GPU>());
    futures.emplace_back(gpus.back().run_async(&GPU::run, queue));
  }

  auto producers = nu::make_distributed_executor(
      +[](decltype(input_rng) &input_rng, decltype(queue) queue) {
        while (true) {
          auto elem = input_rng.pop();
          if (!elem) {
            break;
          }
          compute_us<kProcessTime>();
          queue.push(std::move(*elem));
        }
      },
      input_rng, queue);

  nu::Time::sleep(nu::kOneSecond * 2);
  for (std::size_t i = 0; i < kNumScaleUps; ++i) {
    nu::Time::sleep(nu::kOneSecond * 1);
    for (std::size_t i = 0; i < kNumTempGPUs; ++i) {
      gpus.emplace_back(nu::make_proclet<GPU>());
      futures.emplace_back(gpus.back().run_async(&GPU::run, queue));
    }

    nu::Time::sleep(kScaleUpDurationUs);
    for (std::size_t i = kNumGPUs; i < gpus.size(); ++i) {
      gpus[i].run(&GPU::stop);
    }
    futures.resize(kNumGPUs);
    gpus.resize(kNumGPUs);
  }

  producers.get();
  for (auto &gpu : gpus) {
    gpu.run(&GPU::drain_and_stop);
  }
  for (auto &f : futures) {
    f.get();
  }
}

void do_work() {
  // bench_vector_task_range();
  // bench_rate_match_with_int_vec();
  bench_temp_gpu_slot();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
