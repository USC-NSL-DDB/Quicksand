#include <random>
#include <string>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_unordered_map.hpp"
#include "nu/utils/farmhash.hpp"

using namespace nu;

constexpr static std::size_t kNumElements = 500'000;

struct CustomHasher {
  uint64_t operator()(const std::size_t &s) const {
    return util::Hash64(reinterpret_cast<const char *>(&s), sizeof(s));
  }
};

bool test_size_and_clear() {
  auto sm =
      make_sharded_unordered_map<std::size_t, std::size_t, CustomHasher>();
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

  auto sm = make_sharded_unordered_map<std::size_t, std::size_t>();

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

bool test_erase() {
  auto sm = make_sharded_unordered_map<std::size_t, std::size_t>();

  for (std::size_t i = 0; i < kNumElements; i++) {
    sm.insert(i, i);
  }
  if (!sm.erase(1)) {
    return false;
  }
  if (sm.size() != kNumElements - 1) {
    return false;
  }
  if (sm.find_data(1)) {
    return false;
  }
  if (!sm.find_data(0) || !sm.find_data(2)) {
    return false;
  }
  return true;
}

bool test_apply_on() {
  auto sm = make_sharded_unordered_map<std::size_t, std::size_t>();

  for (std::size_t i = 0; i < kNumElements; i++) {
    sm.insert(i, i);
  }
  auto ret = sm.apply_on(
      1, +[](std::size_t *v) { return ++(*v); });
  if (ret != 2 || sm.find_data(1)->second != 2) {
    return false;
  }
  if (sm.find_data(0)->second != 0 || sm.find_data(2)->second != 2) {
    return false;
  }
  if (sm.find_data(kNumElements)) {
    return false;
  }
  sm.apply_on(
      kNumElements, +[](std::size_t &v) { v = kNumElements; });
  if (sm.find_data(kNumElements)->second != kNumElements) {
    return false;
  }

  return true;
}

bool run_test() {
  return test_size_and_clear() && test_iter() && test_erase() &&
         test_apply_on();
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
