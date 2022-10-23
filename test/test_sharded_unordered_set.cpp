#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_unordered_set.hpp"

using namespace nu;

constexpr static std::size_t kNumElements = 4'000'000;

bool test_insertion() {
  std::unordered_set<std::size_t> expected;
  std::unordered_set<std::size_t> iterated;
  auto s = make_sharded_unordered_set<std::size_t, std::false_type>();

  for (std::size_t i = 0; i < kNumElements; ++i) {
    s.emplace(i);
    expected.emplace(i);
  }

  auto sealed_set = to_sealed_ds(std::move(s));
  for (auto it = sealed_set.cbegin(); it != sealed_set.cend(); ++it) {
    iterated.emplace(*it);
  }

  return iterated == expected;
}

bool test_size() {
  auto s = make_sharded_unordered_set<std::size_t, std::false_type>();

  if (s.size() != 0) return false;

  for (std::size_t i = 0; i < kNumElements; i++) {
    s.emplace(i);
  }
  if (s.size() != kNumElements) return false;

  for (std::size_t i = 0; i < kNumElements; i++) {
    s.emplace(i);
  }
  if (s.size() != kNumElements) return false;

  return true;
}

bool test_clear() {
  auto s = make_sharded_unordered_set<std::size_t, std::false_type>();

  for (std::size_t i = 0; i < kNumElements; i++) {
    s.emplace(i);
  }
  s.clear();

  return s.size() == 0;
}

bool run_test() { return test_insertion() && test_size() && test_clear(); }

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
