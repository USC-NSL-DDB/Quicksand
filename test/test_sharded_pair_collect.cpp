#include <cereal/types/string.hpp>
#include <iostream>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_pair_collect.hpp"

bool run_test() {
  nu::ShardedPairCollection<int, std::string> sc;
  sc.emplace_back(1, std::string("1"));
  std::vector<std::pair<int, std::string>> vec{
      std::pair(2, "2"), std::pair(3, "3"), std::pair(4, "4")};
  sc.emplace_back_batch(vec);
  sc.for_all(
      +[](std::pair<const int, std::string> &p, char new_c) {
        p.second += new_c;
      },
      ' ');
  auto v = sc.collect();
  std::vector<std::pair<int, std::string>> expected_v{
      std::pair(1, "1 "), std::pair(2, "2 "), std::pair(3, "3 "),
      std::pair(4, "4 ")};
  return v == expected_v;
  return true;
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
