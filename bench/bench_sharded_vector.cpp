#include <iostream>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_vector.hpp"

constexpr uint32_t kNumElements = 1 << 28;
constexpr uint32_t kNumVectors = 4;
using T = uint64_t;

struct Bench {
 public:
  void run_sharded_vector() {
    std::cout << "run_sharded_vector()..." << std::endl;

    using ShardedVec = nu::ShardedVector<T, std::false_type>;
    std::vector<ShardedVec> src_vecs;
    for (uint64_t i = 0; i < kNumVectors; i++) {
      src_vecs.emplace_back(nu::make_sharded_vector<T, std::false_type>());
    }

    barrier();
    auto t0 = microtime();
    barrier();

    for (uint64_t i = 0; i < kNumElements; i++) {
      for (auto &vec : src_vecs) {
        vec.push_back(T());
      }
    }

    barrier();
    auto t1 = microtime();
    barrier();

    std::vector<nu::SealedDS<ShardedVec>> sealed_vecs;
    std::vector<nu::SealedDS<ShardedVec>::ConstIterator> iters;
    for (uint64_t i = 0; i < kNumVectors; i++) {
      sealed_vecs.emplace_back(nu::to_sealed_ds(std::move(src_vecs[i])));
      iters.emplace_back(sealed_vecs.back().cbegin());
    }

    barrier();
    auto t2 = microtime();
    barrier();

    while (iters[0] != sealed_vecs[0].cend()) {
      for (uint64_t i = 0; i < kNumVectors; i++) {
        ACCESS_ONCE(*iters[i]);
        ++iters[i];
      }
    }

    barrier();
    auto t3 = microtime();
    barrier();

    std::cout << "\ttime: " << t1 - t0 << " " << t2 - t1 << " " << t3 - t2
              << std::endl;
  }

  void run_std_vector() {
    std::cout << "run_std_vector()..." << std::endl;

    std::vector<T> src_vecs[kNumVectors];

    barrier();
    auto t0 = microtime();
    barrier();

    for (uint64_t i = 0; i < kNumElements; i++) {
      for (auto &vec : src_vecs) {
        vec.push_back(T());
      }
    }

    barrier();
    auto t1 = microtime();
    barrier();

    std::vector<T>::const_iterator iters[kNumVectors];
    for (uint64_t i = 0; i < kNumVectors; i++) {
      iters[i] = src_vecs[i].cbegin();
    }

    while (iters[0] != src_vecs[0].cend()) {
      for (uint64_t i = 0; i < kNumVectors; i++) {
        ACCESS_ONCE(*iters[i]++);
      }
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
