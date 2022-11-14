#include <cereal/types/memory.hpp>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <random>
#include <ranges>

#include "nu/compute_proclet.hpp"
#include "nu/ranges.hpp"
#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_unordered_set.hpp"
#include "nu/sharded_vector.hpp"

constexpr auto kNumElements = 100000;

std::random_device rd;
std::mt19937 mt(rd());
std::uniform_int_distribution<int> dist('A', 'z');

std::string random_str(uint32_t len) {
  std::string str = "";
  for (uint32_t i = 0; i < len; i++) {
    str += dist(mt);
  }
  return str;
}

nu::ShardedVector<std::string, std::false_type> make_sharded_str_vec(
    uint32_t size) {
  int test_str_len = 35;
  auto v = nu::make_sharded_vector<std::string, std::false_type>();
  for (uint32_t i = 0; i < size; ++i) {
    v.emplace_back(random_str(test_str_len));
  }
  return v;
}

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
  auto input = nu::make_sharded_vector<int, std::false_type>(kNumElements);
  auto output = nu::make_sharded_vector<int, std::false_type>();

  for (auto i : std::views::iota(0, kNumElements)) {
    input.push_back(i);
  }
  auto sealed_input = nu::to_sealed_ds(std::move(input));
  auto range_input = nu::range(sealed_input);
  auto cp = nu::compute_range(
      +[](const int &val, decltype(output) &output) { output.push_back(val); },
      range_input, output);

  auto sealed_output = nu::to_sealed_ds(std::move(output));
  if (sealed_output.size() != kNumElements) {
    return false;
  }

  for (const auto &[got, expected] : nu::zip(sealed_input, sealed_output)) {
    if (got != expected) {
      return false;
    }
  }

  return true;
}

bool test_zipped_ints() {
  auto make_sealed_test_data = [&]() {
    auto v = nu::make_sharded_vector<int, std::false_type>(kNumElements);
    for (std::size_t i = 0; i < kNumElements; ++i) {
      v.push_back(1);
    }
    return nu::to_sealed_ds(std::move(v));
  };

  auto sealed_v1 = make_sealed_test_data();
  auto sealed_v2 = make_sealed_test_data();
  auto sum = nu::make_sharded_vector<int, std::false_type>();
  auto inserter = sum.back_inserter();

  auto cp = nu::compute_range(
      +[](const std::tuple<const int &, const int &> &elems,
          decltype(inserter) &out) {
        auto [x, y] = elems;
        out.push_back(x + y);
      },
      nu::zip(sealed_v1, sealed_v2), std::move(inserter));
  cp.get();

  auto sealed_sum = nu::to_sealed_ds(std::move(sum));

  if (sealed_sum.size() != kNumElements) {
    return false;
  }

  for (const auto sum : sealed_sum) {
    if (sum != 2) {
      return false;
    }
  }

  return true;
}

bool test_zipped_strs() {
  auto s1 = make_sharded_str_vec(kNumElements);
  auto s2 = make_sharded_str_vec(kNumElements);
  auto sealed_s1 = nu::to_sealed_ds(std::move(s1));
  auto sealed_s2 = nu::to_sealed_ds(std::move(s2));
  auto set = nu::make_sharded_unordered_set<std::string, std::false_type>();

  std::unordered_set<std::string> expected, got;
  for (const auto &[x, y] : nu::zip(sealed_s1, sealed_s2)) {
    expected.emplace(x + y);
  }

  auto input = nu::zip(sealed_s1, sealed_s2);
  auto cp = nu::compute_range(
      +[](const std::tuple<const std::string &, const std::string &> &elems,
          decltype(set) &set) {
        auto [s1, s2] = elems;
        set.emplace(s1 + s2);
      },
      input, set);
  cp.get();

  auto sealed_set = nu::to_sealed_ds(std::move(set));
  for (const auto &elem : sealed_set) {
    got.emplace(elem);
  }

  return got == expected;
}

bool test_compute_over_zipped_range() {
  return test_zipped_ints() && test_zipped_strs();
}

bool run_test() {
  auto passed = test_basic() && test_pass_sharded_ds() &&
                test_compute_over_sharded_ds() &&
                test_compute_over_zipped_range();
  return passed;
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
