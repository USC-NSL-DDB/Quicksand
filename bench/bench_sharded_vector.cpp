#include <algorithm>
#include <cereal/types/string.hpp>
#include <functional>
#include <iostream>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_vector.hpp"

constexpr uint32_t kNumElements = 10 << 25;
constexpr uint32_t kRunTimes = 1;
constexpr uint32_t kPowerShardSize = 20;

// template <typename F>
// inline uint64_t time(F fn) {
//   auto t0 = microtime();
//   fn();
//   auto t1 = microtime();
//   return t1 - t0;
// }
//
// template <typename F>
// inline void time_sharded_vec(F fn) {
//   auto t = time(fn);
//   std::cout << "\t\tShardedVector: " << t << std::endl;
// }
//
// template <typename F>
// inline void time_std_vec(F fn) {
//   auto t = time(fn);
//   std::cout << "\t\tstd::vector: " << t << std::endl;
// }
//
// template <typename T>
// void bench_push_back(uint32_t num_elems, const T &elem) {
//   {
//     auto v = nu::make_sharded_vector<T>(kPowerShardSize);
//     auto insertion_time = time([&]() {
//       for (uint32_t i = 0; i < num_elems; i++) {
//         v.push_back(elem);
//       }
//     });
//     std::cout << "\t\tShardedVector:\t" << insertion_time << " us" <<
//     std::endl;
//   }
//
//   {
//     nu::RuntimeSlabGuard slab;
//     std::vector<T> v;
//     auto insertion_time = time([&]() {
//       for (uint32_t i = 0; i < num_elems; i++) {
//         v.push_back(elem);
//       }
//     });
//     std::cout << "\t\tstd::vector:\t" << insertion_time << " us" <<
//     std::endl;
//   }
// }
//
// void single_thread_push_back_int() {
//   std::cout << "\tRunning single-thread push_back int..." << std::endl;
//   bench_push_back(kNumElements, 33);
// }
//
// void single_thread_push_back_string() {
//   std::cout << "\tRunning single-thread push_back string..." << std::endl;
//   std::string s = "hello world";
//   bench_push_back(kNumElements, s);
// }
//
// void single_thread_seq_read() {
//   std::cout << "\tRunning single-thread sequential read..." << std::endl;
//   {
//     auto sv = nu::make_sharded_vector<int>(kPowerShardSize);
//     for (uint32_t i = 0; i < kNumElements; i++) {
//       sv.push_back(1);
//     }
//     int x = 0;
//     time_sharded_vec([&]() {
//       for (uint32_t i = 0; i < kNumElements; i++) {
//         x += sv[i];
//       }
//     });
//     assert(x == kNumElements);
//
//     x = 0;
//     time_sharded_vec([&]() {
//       x = sv.reduce(
//           0, +[](int sum, int x) { return sum + x; });
//     });
//     assert(x == kNumElements);
//   }
//
//   {
//     nu::RuntimeSlabGuard slab;
//     std::vector<int> v(kNumElements, 1);
//     int x = 0;
//     time_std_vec([&]() {
//       for (uint32_t i = 0; i < kNumElements; i++) {
//         x += v[i];
//       }
//     });
//     std::cout << "\t\t\toutput: " << x << std::endl;
//   }
// }
//
// void single_thread_seq_write() {
//   std::cout << "\tRunning single-thread sequential write..." << std::endl;
//   {
//     auto sv = nu::make_sharded_vector<int>(kPowerShardSize);
//     for (uint32_t i = 0; i < kNumElements; i++) {
//       sv.push_back(1);
//     }
//     time_sharded_vec([&]() { sv.for_all(+[](int x) { return x * 2; }); });
//   }
//
//   {
//     nu::RuntimeSlabGuard slab;
//     std::vector<int> v(kNumElements, 1);
//     time_std_vec([&]() {
//       for (uint32_t i = 0; i < kNumElements; i++) {
//         v[i] = v[i] * 2;
//       }
//     });
//   }
// }

// class SingleNodeWork {
//  public:
//   SingleNodeWork() {
//     std::cout << "Num elements: " << kNumElements << std::endl;
//     for (uint32_t i = 0; i < kRunTimes; i++) {
//       std::cout << "Running No." << i << " time..." << std::endl;
//
//       single_thread_push_back_int();
//       single_thread_push_back_string();
//       single_thread_seq_read();
//       single_thread_seq_write();
//     }
//   }
// };

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    // nu::make_proclet<SingleNodeWork>();

    // single_thread_push_back_int();
    // single_thread_push_back_string();
    // single_thread_seq_read();
    // single_thread_seq_write();
  });
}
