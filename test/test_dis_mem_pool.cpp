#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <cstdint>
#include <iostream>
#include <memory>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "dis_mem_pool.hpp"
#include "rem_obj.hpp"
#include "runtime.hpp"

using namespace nu;

Runtime::Mode mode;

constexpr static uint32_t kNumThreads = 100;
constexpr static uint32_t kNumAllocationsPerThread = 1000000;

bool run_single_thread() {
  std::vector<int> a{1, 2, 3, 4, 5, 6};

  auto rem_obj = RemObj<ErasedType>::create();
  auto [dis_mem_pool, rem_raw_ptr] = rem_obj.run(
      +[](ErasedType &, std::vector<int> a) {
        DistributedMemPool dis_mem_pool;
        return std::make_pair(std::move(dis_mem_pool),
                              dis_mem_pool.allocate_raw<std::vector<int>>(a));
      },
      a);

  auto rem_unique_ptr = dis_mem_pool.allocate_unique<std::vector<int>>(a);
  auto rem_shared_ptr = dis_mem_pool.allocate_shared<std::vector<int>>(a);

  if (a != *rem_raw_ptr) {
    return false;
  }
  dis_mem_pool.free_raw(rem_raw_ptr);

  if (a != *rem_unique_ptr) {
    return false;
  }

  if (a != *rem_shared_ptr) {
    return false;
  }

  std::vector<RemUniquePtr<int>> unique_ptrs;
  for (uint64_t i = 0; i < DistributedMemPool::kShardSize / sizeof(int); i++) {
    unique_ptrs.emplace_back(std::move(dis_mem_pool.allocate_unique<int>()));
  }

  return true;
}

bool run_multi_thread() {
  DistributedMemPool dis_mem_pool;
  std::vector<rt::Thread> alloc_threads;
  std::vector<rt::Thread> check_threads;
  std::vector<RemUniquePtr<std::pair<uint64_t, uint64_t>>>
      unique_ptrs[kNumThreads];

  for (uint32_t i = 0; i < kNumThreads; i++) {
    alloc_threads.emplace_back([tid = i, &unique_ptrs, &dis_mem_pool] {
      for (uint32_t j = 0; j < kNumAllocationsPerThread; j++) {
        unique_ptrs[tid].emplace_back(std::move(
            dis_mem_pool.allocate_unique<std::pair<uint64_t, uint64_t>>(tid,
                                                                        j)));
      }
    });
  }
  for (auto &alloc_thread : alloc_threads) {
    alloc_thread.Join();
  }

  bool match = true;
  for (uint32_t i = 0; i < kNumThreads; i++) {
    check_threads.emplace_back([tid = i, &match, &unique_ptrs] {
      for (uint32_t j = 0; j < kNumAllocationsPerThread; j++) {
        if (*unique_ptrs[tid][j] !=
            std::make_pair<uint64_t, uint64_t>(tid, j)) {
          match = false;
        }
        unique_ptrs[tid][j].reset();
      }
    });
  }
  for (auto &check_thread : check_threads) {
    check_thread.Join();
  }

  return ACCESS_ONCE(match);
}

bool run_test() { return run_single_thread() && run_multi_thread(); }

void do_work() {
  if (run_test()) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
