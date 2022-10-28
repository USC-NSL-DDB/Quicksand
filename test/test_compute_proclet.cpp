#include <cereal/types/memory.hpp>
#include <cstdint>
#include <iostream>

#include "nu/compute_proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_unordered_map.hpp"
#include "nu/sharded_vector.hpp"

#define RUN_TEST(test) \
  do {                 \
    if (!test()) {     \
      return false;    \
    }                  \
  } while (0);

bool test_basic_compute_proclet() {
  auto test_concat = [](auto a, auto b) {
    auto expected = a + b;
    auto cp = nu::compute([](auto &x, auto &y) { return x + y; }, std::move(a),
                          std::move(b));
    return cp.get() == expected;
  };

  // TODO: not passing b/c of segfault when deserializing RPC args?
  // return test_concat(std::string{"hello"}, std::string{"world"});
  return test_concat(1, 2);
}

bool test_pass_proclet_arg() {
  auto ints = nu::make_sharded_vector<int, std::false_type>();
  auto ints_ref = ints;
  auto cp = nu::compute([](auto &ints) { ints.push_back(1); }, std::move(ints));
  cp.get();
  auto result = ints_ref[0];
  return result == 1;
}

bool test_compute_proclet_over_one_sharded_ds() {
  std::size_t elem_count = 10;

  auto input = nu::make_sharded_vector<int, std::false_type>(elem_count);
  auto output = nu::make_sharded_vector<int, std::false_type>();

  for (std::size_t i = 0; i < elem_count; ++i) {
    input.push_back(1);
  }
  auto sealed_in = nu::to_sealed_ds(std::move(input));

  auto out_ref = output;
  auto cp =
      nu::compute_range([](auto &elem, auto &out) { out.push_back(elem); },
                        nu::range(sealed_in), std::move(out_ref));
  cp.get();

  auto sealed_out = nu::to_sealed_ds(std::move(output));
  if (sealed_out.size() != 10) {
    return false;
  }
  for (const auto elem : sealed_out) {
    if (elem != 1) {
      return false;
    }
  }

  return true;
}

// TODO: implement these tests
// bool test_adding_two_vectors() {
//   std::size_t elem_count = 1'000'000;
//
//   auto make_sealed_test_data = [&]() {
//     auto v = nu::make_sharded_vector<int, std::false_type>(elem_count);
//     for (std::size_t i = 0; i < elem_count; ++i) {
//       v.push_back(1);
//     }
//     return nu::to_sealed_ds(std::move(v));
//   };
//
//   auto sealed_v1 = make_sealed_test_data();
//   auto sealed_v2 = make_sealed_test_data();
//   auto sum = nu::make_sharded_vector<int, std::false_type>();
//
//   auto cp = nu::compute_range(
//       [](auto &elems, auto &out) {
//         auto [x, y] = elems;
//         out.push_back(x + y);
//       },
//       nu::zip(sealed_v1, sealed_v2), std::move(sum));
//   cp.get();
//
//   return true;
// }

// bool test_naive_chained_compute() {
//   auto first = nu::make_sharded_vector<std::string, std::false_type>();
//   auto last = nu::make_sharded_vector<std::string, std::false_type>();
//   auto name_count =
//       nu::make_sharded_unordered_map<std::string, int, std::false_type>();
//
//   auto names = nu::make_sharded_vector<std::string, std::false_type>();
//   auto names_ref = names;
//   auto cp1 = nu::compute_range(
//       [](auto &elems, auto &out) {
//         auto [first, last] = elems;
//         out.push_back(first + " " + last);
//       },
//       nu::zip(sealed_first, sealed_last), std::move(names_ref));
//   cp1.get();
//
//   auto name_count_ref = name_count;
//   auto cp2 = nu::compute_range([](auto &name, auto &count) { count[name]++;
//   },
//                                nu::range(nu::to_sealed_ds(std::move(names))),
//                                std::move(name_count_ref));
//   cp2.get();
// }
//
// bool test_optimized_chained_compute() {
//   auto first = nu::make_sharded_vector<std::string, std::false_type>();
//   auto last = nu::make_sharded_vector<std::string, std::false_type>();
//   auto name_count =
//       nu::make_sharded_unordered_map<std::string, int, std::false_type>();
//
//   auto name_count_ref = name_count;
//   auto cp = nu::compute_range(
//       [](auto &elems, auto &counts) {
//         auto [first, last] = elems;
//         counts[first + " " + last]++;
//       },
//       nu::zip(sealed_first, sealed_last), std::move(name_count_ref));
//   cp.get();
// }
//
// bool test_chained_compute() {
//   return test_chained_compute() && test_optimized_chained_compute();
// }

bool run_test() {
  RUN_TEST(test_basic_compute_proclet);
  // RUN_TEST(test_pass_proclet_arg);
  //
  // TODO: sometimes segfaults in PressureHandler, when inserting into
  // PressureHandler->new_cpu_pressure_sorted_proclets;
  // RUN_TEST(test_compute_proclet_over_one_sharded_ds);

  return true;
}

void do_work() {
  if (run_test()) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
