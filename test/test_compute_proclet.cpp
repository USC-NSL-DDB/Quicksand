#include <cereal/types/memory.hpp>
#include <cstdint>
#include <iostream>
#include <ranges>

#include "nu/compute_proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_unordered_map.hpp"
#include "nu/sharded_vector.hpp"

bool test_basic() {
  std::string s0{"hello"};
  std::string s1{"world"};

  auto cp = nu::make_compute_proclet(
      +[](std::string s0, std::string s1) { return s0 + s1; }, s0, s1);
  return cp.get() == s0 + s1;
}

bool test_pass_sharded_ds() {
  auto ints = nu::make_sharded_vector<int, std::false_type>();
  using Ints = decltype(ints);
  auto cp = nu::make_compute_proclet(
      +[](Ints ints) { ints.push_back(1); }, ints);
  cp.get();
  return ints[0] == 1;
}

bool test_compute_over_sharded_ds() {
  constexpr auto kNumElements = 10;

  auto input = nu::make_sharded_vector<int, std::false_type>(kNumElements);
  auto output = nu::make_sharded_vector<int, std::false_type>();

  for (auto i : std::views::iota(0, kNumElements)) {
    input.push_back(i);
  }
  auto sealed_input = nu::to_sealed_ds(std::move(input));
  auto range_input = nu::range(sealed_input);
  auto cp = nu::compute_range(
      +[](const int &val, decltype(output) output) { output.push_back(val); },
      range_input, output);

  auto sealed_output = nu::to_sealed_ds(std::move(output));
  if (sealed_output.size() != kNumElements) {
    return false;
  }

  // TODO: support std::ranges::zip_view.
  auto it_input = sealed_input.cbegin();
  auto it_output = sealed_output.cbegin();
  for (; it_input != sealed_input.cend(); ++it_input, ++it_output) {
    if (*it_input != *it_output) {
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
  return test_basic() && test_pass_sharded_ds() &&
         test_compute_over_sharded_ds();
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
