#include <iostream>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_vector.hpp"
#include "nu/utils/time.hpp"

constexpr uint64_t kTotalMemUsage = 15ULL * 1024 * 1024 * 1024;
constexpr uint64_t kNumComputeThreads = 26;
constexpr uint64_t kElementSize = 1000;
constexpr uint64_t kNumElements =
    kTotalMemUsage / kNumComputeThreads / kElementSize;
constexpr uint64_t kDelayTimeNs = 30000;

struct Element {
  uint64_t data[kElementSize / sizeof(uint64_t)];
};
using ShardedVec = nu::ShardedVector<Element, std::false_type>;

void compute_on(const Element &e) {
  ACCESS_ONCE(e.data[0]);
  nu::Time::delay_ns(kDelayTimeNs);
}

struct Worker {
  Worker(ShardedVec sv)
      : sharded_vec(std::move(sv)),
        sealed_vec(nu::to_sealed_ds(std::move(sharded_vec))) {}

  void do_work() {
    for (const auto &element : sealed_vec) {
      compute_on(element);
    }
  }

  ShardedVec sharded_vec;
  nu::SealedDS<ShardedVec> sealed_vec;
};

void run_sharded_vector() {
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
    futures.emplace_back(computer.run_async(&Worker::do_work));
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

  std::cout << t1 - t0 << " us" << std::endl;
}

void run_std_vector() {
  std::vector<Element> std_vecs[kNumComputeThreads];

  {
    std::vector<nu::Thread> threads;
    for (uint64_t i = 0; i < kNumComputeThreads; i++) {
      threads.emplace_back(nu::Thread([std_vec_ptr = &std_vecs[i]] {
        for (uint64_t i = 0; i < kNumElements; i++) {
          std_vec_ptr->push_back(Element());
        }
      }));
    }
    for (auto &thread : threads) {
      thread.join();
    }
  }

  std::vector<nu::Thread> threads;

  for (uint64_t i = 0; i < kNumComputeThreads; i++) {
    threads.emplace_back(nu::Thread([std_vec_ptr = &std_vecs[i]] {
      for (const auto &element : *std_vec_ptr) {
        compute_on(element);
      }
    }));
  }

  barrier();
  auto t0 = microtime();
  barrier();

  for (auto &thread : threads) {
    thread.join();
  }

  barrier();
  auto t1 = microtime();
  barrier();

  std::cout << t1 - t0 << " us" << std::endl;
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [&](int, char **) {
    run_sharded_vector();
    run_std_vector();
  });
}
