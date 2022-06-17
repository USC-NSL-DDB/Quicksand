#include <math.h>

#include <cereal/types/set.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/unordered_map.hpp>
#include <cereal/types/utility.hpp>
#include <cstdint>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <utility>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "nu/dis_vector.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/farmhash.hpp"

using namespace nu;

Runtime::Mode mode;

#define ABORT_IF_FAILED(passed) \
  do {                          \
    if (!passed) {              \
      return false;             \
    }                           \
  } while (0);

#define TEST(cond)  \
  do {              \
    if (!(cond)) {  \
      return false; \
    }               \
  } while (0);

bool run_test() {
  uint32_t power_shard_sz = 10;

  auto vec = make_dis_vector<int>(power_shard_sz);
  TEST(vec.empty());

  for (int i = 0; i < 1000; i++) {
    vec.push_back(i);
    TEST(vec.size() == (size_t)i + 1);
    TEST(!vec.empty());
  }
  for (int i = 0; i < 1000; i++) {
    TEST(vec[i] == i);
  }
  for (int i = 0; i < 1000; i++) {
    vec[i] = 1000 - i;
  }
  for (int i = 0; i < 1000; i++) {
    TEST(vec[i] == 1000 - i);
  }

  TEST(!vec.empty());

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
