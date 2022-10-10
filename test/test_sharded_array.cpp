#include <iostream>
#include <string>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_array.hpp"

using namespace nu;

bool test_write_read() {
  auto target_size = 10'000'000;
  auto arr = make_sharded_array<int, std::false_type>(target_size);

  for (auto i = 0; i < target_size; i++) {
    arr.set(i, i);
  }

  auto sealed_arr = nu::to_sealed_ds(std::move(arr));
  int i = 0;
  for (auto v : sealed_arr) {
    if (v != i++) {
      return false;
    }
  }

  return true;
}

bool test_iter() {
  auto target_size = 10'000'000;
  auto arr = make_sharded_array<int, std::false_type>(target_size);

  for (auto i = 0; i < target_size; ++i) {
    arr.set(i, i);
  }

  auto sealed_arr = to_sealed_ds(std::move(arr));
  auto expected = 0;
  for (auto elem : sealed_arr) {
    if (elem != expected++) {
      return false;
    }
  }

  expected--;
  for (auto it = sealed_arr.crbegin(); it != sealed_arr.crend(); ++it) {
    if (*it != expected--) {
      return false;
    }
  }

  return true;
}

bool run_test() {
  if (!test_write_read()) {
    return false;
  }

  if (!test_iter()) {
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
