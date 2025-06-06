#include <iostream>
#include <string>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_vector.hpp"

using namespace nu;

bool test_push_and_set() {
  constexpr uint32_t kSize = 50 << 20;
  auto vec = make_sharded_vector<int, std::false_type>(40 << 20);

  for (size_t i = 0; i < kSize; i++) {
    vec.push_back(i);
  }

  {
    auto sealed_vec = nu::to_sealed_ds(std::move(vec));
    int i = 0;
    for (auto it = sealed_vec.cbegin(); it != sealed_vec.cend(); ++it, ++i) {
      if (*it != i) {
        return false;
      }
    }
    vec = nu::to_unsealed_ds(std::move(sealed_vec));
  }

  for (size_t i = 0; i < kSize; i++) {
    vec.set(i, 2 * i);
  }

  {
    auto sealed_vec = nu::to_sealed_ds(std::move(vec));
    int i = 0;
    for (auto it = sealed_vec.cbegin(); it != sealed_vec.cend(); ++it, ++i) {
      if (*it != 2 * i) {
        return false;
      }
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

  {
    auto sealed_vec = nu::to_sealed_ds(std::move(vec));
    int i = 0;
    for (auto it = sealed_vec.cbegin(); it != sealed_vec.cend(); ++it, ++i) {
      if (*it != 2 * i) {
        return false;
      }
    }
  }

  return true;
}

bool test_concat() {
  constexpr int kSize = 50 << 20;
  auto vec0 = make_sharded_vector<int, std::false_type>();
  auto vec1 = make_sharded_vector<int, std::false_type>();

  for (int i = 0; i < kSize; i++) {
    vec0.push_back(i);
    vec1.push_back(i + kSize);
  }

  vec0.concat(std::move(vec1));
  auto sealed_vec = nu::to_sealed_ds(std::move(vec0));
  if (sealed_vec.size() != kSize * 2) {
    return false;
  }

  int idx = 0;
  for (const auto &d: sealed_vec) {
    if (d != idx++) {
      return false;
    }
  }
  return idx == kSize * 2;
}

bool run_test() {
  return test_push_and_set() && test_vec_clear() && test_for_all() &&
         test_concat();
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
