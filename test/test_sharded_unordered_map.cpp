#include <random>
#include <string>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_unordered_map.hpp"

using namespace nu;

constexpr static std::size_t kNumElements = 4'000'000;

bool test_size_and_clear() {
  auto sm =
      make_sharded_unordered_map<std::size_t, std::size_t, std::false_type>();
  if (sm.size() != 0) return false;

  for (std::size_t i = 0; i < kNumElements; i++) {
    sm.insert(i, i);
  }
  if (sm.size() != kNumElements) return false;

  sm.clear();
  if (sm.size() != 0) return false;

  return true;
}

bool test_iter() {
  std::unordered_map<std::size_t, std::size_t> expected;
  std::unordered_map<std::size_t, std::size_t> iterated;

  auto sm =
      make_sharded_unordered_map<std::size_t, std::size_t, std::false_type>();

  for (std::size_t i = 0; i < kNumElements; i++) {
    sm.insert(i, i);
    expected.emplace(i, i);
  }

  auto sealed_sm = to_sealed_ds(std::move(sm));

  for (auto it = sealed_sm.cbegin(); it != sealed_sm.cend(); ++it) {
    iterated.insert(*it);
  }

  return expected == iterated;
}

bool run_test() {
  return test_size_and_clear() && test_iter();
}

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
