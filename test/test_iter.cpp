#include <cstdint>
#include <iostream>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_vector.hpp"

using namespace nu;

constexpr uint32_t kNumElements = 1 << 20;

bool run_test() {
  auto vec = make_sharded_vector<int, std::false_type>();
  for (uint32_t i = 0; i < kNumElements; ++i) {
    vec.push_back(i);
  }
  auto sealed_vec = to_sealed_ds(std::move(vec));

  int idx = 0;
  for (auto iter = sealed_vec.cbegin(); iter != sealed_vec.cend();
       ++iter, ++idx) {
    if (*iter != idx) {
      return false;
    }
  }

  --idx;
  for (auto iter = --sealed_vec.cend(); iter != sealed_vec.cbegin();
       --iter, --idx) {
    if (*iter != idx) {
      return false;
    }
  }
  if (*sealed_vec.cbegin() != idx) {
    return false;
  }

  for (auto iter = --sealed_vec.crend(); iter != sealed_vec.crbegin();
       --iter, ++idx) {
    if (*iter != idx) {
      return false;
    }
  }
  if (*sealed_vec.crbegin() != idx) {
    return false;
  }

  for (auto iter = sealed_vec.crbegin(); iter != sealed_vec.crend();
       ++iter, --idx) {
    if (*iter != idx) {
      return false;
    }
  }
  vec = to_unsealed_ds(std::move(sealed_vec));

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
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
