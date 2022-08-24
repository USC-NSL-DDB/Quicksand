#include <cstdint>
#include <iostream>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_vector.hpp"

using namespace nu;

bool run_test() {
  auto vec = make_sharded_vector<int, std::false_type>();
  auto sealed_vec = make_sealed_ds(std::move(vec));

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
