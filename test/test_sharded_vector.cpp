#include <cereal/types/string.hpp>
#include <iostream>
#include <string>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_vector.hpp"

using namespace nu;

bool test_push() {
  constexpr uint32_t kSize = 50 << 20;
  auto vec = make_sharded_vector<int, std::false_type>(40 << 20);

  for (size_t i = 0; i < kSize; i++) {
    vec.push_back(i);
  }

  auto sealed_vec = nu::to_sealed_ds(std::move(vec));
  int i = 0;
  for (auto it = sealed_vec.cbegin(); it != sealed_vec.cend(); ++it, ++i) {
    if (*it != i) {
      return false;
    }
  }

  return true;
}

bool test_vec_clear() {
  constexpr uint32_t kSize = 10 << 20;
  auto vec = make_sharded_vector<int, std::false_type>();

  if (!vec.empty()) {
    return false;
  }

  for (size_t i = 0; i < kSize; i++) {
    vec.push_back(i);
  }

  if (vec.empty()) {
    return false;
  }
  vec.clear();
  if (!vec.empty()) {
    return false;
  }

  return true;
}

bool test_for_all() {
  constexpr int kSize = 10 << 20;
  auto vec = make_sharded_vector<int, std::false_type>();

  for (int i = 0; i < kSize; i++) {
    vec.push_back(i);
  }
  vec.for_all(+[](const std::size_t &idx, int &val) { val *= 2; });
  for (int i = 0; i < kSize; i++) {
    if (vec[i] != i * 2) {
      return false;
    }
  }

  return true;
}

bool run_test() {
  if (!test_push()) {
    return false;
  }
  if (!test_vec_clear()) {
    return false;
  }
  if (!test_for_all()) {
    return false;
  }

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
