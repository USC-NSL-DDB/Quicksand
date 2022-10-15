#include <iostream>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_vector.hpp"

constexpr uint32_t kNumElements = 1 << 28;

struct Bench {
 public:
  void run_sharded_vector() {
    std::cout << "run_sharded_vector()..." << std::endl;

    auto vec = nu::make_sharded_vector<uint64_t, std::false_type>();

    barrier();
    auto t0 = microtime();
    barrier();

    for (uint64_t i = 0; i < kNumElements; i++) {
      vec.push_back(i);
    }

    barrier();
    auto t1 = microtime();
    barrier();

    auto sealed_vec = nu::to_sealed_ds(std::move(vec));
    for (const auto &v : sealed_vec) {
      ACCESS_ONCE(v);
    }

    barrier();
    auto t2 = microtime();
    barrier();

    std::cout << "\ttime: " << t1 - t0 << " " << t2 - t1 << std::endl;
  }

  void run_std_vector() {
    std::cout << "run_std_vector()..." << std::endl;

    std::vector<uint64_t> vec;

    barrier();
    auto t0 = microtime();
    barrier();

    for (uint64_t i = 0; i < kNumElements; i++) {
      vec.push_back(i);
    }

    barrier();
    auto t1 = microtime();
    barrier();

    for (const auto &v : vec) {
      ACCESS_ONCE(v);
    }

    barrier();
    auto t2 = microtime();
    barrier();

    std::cout << "\ttime: " << t1 - t0 << " " << t2 - t1 << std::endl;
  }
};

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    Bench b;
    b.run_std_vector();
    b.run_sharded_vector();
  });
}
