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

void bench_temp_gpu_slot() {
  std::cout << __FUNCTION__ << std::endl;

  using Elem = std::vector<char>;
  using GPU = MockGPU<Elem>;

  constexpr uint64_t kProcessTime = 4000;
  constexpr uint64_t kNumGPUs = 4;
  constexpr uint64_t kNumTempGPUs = 4;
  constexpr uint64_t kScaleUpDurationUs = nu::kOneMilliSecond * 1000;
  constexpr uint64_t kNumScaleUps = 10;
  constexpr std::size_t kElemSize = 150'000;
  constexpr std::size_t kNumElems = 1 << 16;

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
    nu::Time::sleep(nu::kOneSecond);
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
  bench_temp_gpu_slot();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
