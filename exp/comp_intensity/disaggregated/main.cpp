#include <iostream>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_vector.hpp"

constexpr uint64_t kTotalMemUsage = 15ULL * 1024 * 1024 * 1024;
constexpr uint64_t kNumComputeThreads = 26;
constexpr uint64_t kElementSize = 100;
constexpr uint64_t kNumElements =
    kTotalMemUsage / kNumComputeThreads / kElementSize;
constexpr uint64_t kDelayUs = 7;

struct Element {
  Element() = default;
  uint64_t data[kElementSize / sizeof(uint64_t)];
};
using ShardedVec = nu::ShardedVector<Element, std::false_type>;

void compute_on(const Element &e) {
  for (const auto &d : e.data) {
    ACCESS_ONCE(d);
  }
  {
    nu::Caladan::PreemptGuard g;

    unsigned long start = rdtsc();
    auto delay_cycles = kDelayUs * cycles_per_us;
    while (rdtsc() - start < delay_cycles) {
      cpu_relax();
    }
  }
}

struct Worker {
  Worker(ShardedVec sv)
      : sharded_vec(std::move(sv)),
        sealed_vec(nu::to_sealed_ds(std::move(sharded_vec))) {}

  void work() {
    for (const auto &element : sealed_vec) {
      compute_on(element);
    }
  }

  ShardedVec sharded_vec;
  nu::SealedDS<ShardedVec> sealed_vec;
};

void run() {
  ShardedVec sharded_vecs[kNumComputeThreads];
  for (uint64_t i = 0; i < kNumComputeThreads; i++) {
    sharded_vecs[i] = nu::make_sharded_vector<Element, std::false_type>();
  }

  {
    std::vector<nu::Thread> threads;
    for (uint64_t i = 0; i < kNumComputeThreads; i++) {
      threads.emplace_back(nu::Thread([sharded_vec_ptr = &sharded_vecs[i]] {
        for (uint64_t i = 0; i < kNumElements; i++) {
          sharded_vec_ptr->push_back(Element());
        }
      }));
    }
    for (auto &thread : threads) {
      thread.join();
    }
  }

  std::vector<nu::Proclet<Worker>> computers;
  for (uint64_t i = 0; i < kNumComputeThreads; i++) {
    computers.emplace_back(
        nu::make_proclet<Worker>(std::make_tuple(std::move(sharded_vecs[i]))));
  }

  std::vector<nu::Future<void>> futures;
  for (auto &computer : computers) {
    futures.emplace_back(computer.run_async(&Worker::work));
  }

  barrier();
  auto t0 = microtime();
  barrier();

  for (auto &future : futures) {
    future.get();
  }

  barrier();
  auto t1 = microtime();
  barrier();

  std::cout << t1 - t0 << std::endl;
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [&](int, char **) { run(); });
}
