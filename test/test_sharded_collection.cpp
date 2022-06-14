#include <iostream>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_collection.hpp"

bool run_test() {
  nu::ShardedCollection<int> sc;
  sc.emplace_back(1);
  std::vector<int> vec{2, 3, 4};
  sc.emplace_back_batch(vec);
  sc.for_all(
      +[](int &x, int delta) { x += delta; }, 1);
  auto v = sc.collect();
  std::vector<int> expected_v{2, 3, 4, 5};
  return v == expected_v;
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    if (run_test()) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });
}
