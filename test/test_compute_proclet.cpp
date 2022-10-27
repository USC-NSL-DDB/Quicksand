#include <iostream>

#include "nu/compute_proclet.hpp"
#include "nu/runtime.hpp"

bool test_basic_compute_proclet() {
  auto test_concat = [](auto a, auto b) {
    auto expected = a + b;
    auto cp = nu::compute([](auto &x, auto &y) { return x + y; }, std::move(a),
                          std::move(b));
    return cp.get() == expected;
  };

  // TODO: not passing b/c of segfault when deserializing RPC args?
  // return test_concat(std::string{"hello"}, std::string{"world"});
  return test_concat(1, 2);
}

bool run_test() { return test_basic_compute_proclet(); }

void do_work() {
  if (run_test()) {
    std::cout << "passed" << std::endl;
  } else {
    std::cout << "failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
