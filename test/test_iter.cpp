#include <cstdint>
#include <iostream>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_vector.hpp"

using namespace nu;

constexpr uint32_t kNumElements = (1 << 20) + 1;

bool test_empty_ds() {
  auto vec = make_sharded_vector<uint32_t, std::false_type>();
  auto sealed_vec = to_sealed_ds(std::move(vec));
  if (sealed_vec.size() != 0) {
    return false;
  }
  auto v = to_unsealed_ds(std::move(sealed_vec));
  return true;
}

bool test_nonempty_ds() {
  auto vec = make_sharded_vector<uint32_t, std::false_type>();
  for (uint32_t i = 0; i < kNumElements; ++i) {
    vec.push_back(i);
  }
  auto sealed_vec = to_sealed_ds(std::move(vec));

  uint32_t idx = 0;
  for (const auto &val : sealed_vec) {
    if (val != idx++) {
      return false;
    }
  }

  --idx;
  {
    auto iter = sealed_vec.cend();
    --iter;
    for (; iter != sealed_vec.cbegin(); --iter, --idx) {
      if (*iter != idx) {
        return false;
      }
    }
    if (*sealed_vec.cbegin() != idx) {
      return false;
    }
  }

  {
    auto iter = sealed_vec.crend();
    --iter;
    for (; iter != sealed_vec.crbegin(); --iter, ++idx) {
      if (*iter != idx) {
        return false;
      }
    }
    if (*sealed_vec.crbegin() != idx) {
      return false;
    }
  }

  {
    for (auto iter = sealed_vec.crbegin(); iter != sealed_vec.crend();
         ++iter, --idx) {
      if (*iter != idx) {
        return false;
      }
    }
  }

  for (uint32_t i = 0; i < kNumElements; i++) {
    if (i != *sealed_vec.find_iter(i)) {
      return false;
    }
  }
  {
    auto iter = sealed_vec.find_iter(kNumElements / 2);
    for (uint32_t i = kNumElements / 2; i < kNumElements; ++i, ++iter) {
      if (i != *iter) {
        return false;
      }
    }
    if (iter != sealed_vec.cend()) {
      return false;
    }
  }

  vec = to_unsealed_ds(std::move(sealed_vec));

  return true;
}

bool run_test() { return test_empty_ds() && test_nonempty_ds(); }

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
