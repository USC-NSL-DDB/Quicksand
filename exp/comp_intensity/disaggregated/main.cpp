#include <iostream>
#include <vector>

#include "nu/dis_executor.hpp"
#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_vector.hpp"
#include "nu/utils/time.hpp"

constexpr uint64_t kTotalMemUsage = 15ULL * 1024 * 1024 * 1024;
constexpr uint64_t kElementSize = 100;
constexpr uint64_t kNumElements = kTotalMemUsage / kElementSize;
constexpr uint64_t kDelayUs = 1;

struct Element {
  Element() = default;
  uint64_t data[kElementSize / sizeof(uint64_t)];
};
using ShardedVec = nu::ShardedVector<Element, std::false_type>;

void compute_on(const Element &e) {
  for (const auto &d : e.data) {
    ACCESS_ONCE(d);
  }
  nu::Time::delay_us(kDelayUs);
}

void run() {
  auto sharded_vec = nu::make_sharded_vector<Element, std::false_type>();
  for (uint64_t i = 0; i < kNumElements; i++) {
    sharded_vec.push_back(Element());
  }
  auto sealed_vec = nu::to_sealed_ds(std::move(sharded_vec));
  auto cont_ds_range = nu::make_contiguous_ds_range(sealed_vec);
  auto dis_exec = nu::make_distributed_executor(
      +[](decltype(cont_ds_range) &task_range) {
        while (true) {
          auto popped = task_range.pop();
          if (!popped) {
            break;
          }
          compute_on(*popped);
        }
      },
      cont_ds_range);

  barrier();
  auto t0 = microtime();
  barrier();

  dis_exec.get();

  barrier();
  auto t1 = microtime();
  barrier();

  std::cout << t1 - t0 << std::endl;
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [&](int, char **) { run(); });
}
