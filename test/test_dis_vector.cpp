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

std::vector<int> make_int_range_vec(int start_incl, int end_excl) {
  BUG_ON(start_incl > end_excl);
  std::vector<int> vec(end_excl - start_incl);
  for (int i = 0; i < end_excl - start_incl; i++) {
    vec[i] = i + start_incl;
  }
  return vec;
}

template <typename T>
bool test_push_and_pop(std::vector<T> expected, uint32_t power_shard_sz) {
  size_t len = expected.size();

  auto vec = make_dis_vector<T>(power_shard_sz);
  TEST(vec.empty());

  for (size_t i = 0; i < len; i++) {
    vec.push_back(expected[i]);
    TEST(vec.size() == i + 1);
    TEST(!vec.empty());
  }
  for (size_t i = 0; i < len; i++) {
    TEST(vec[i] == expected[i]);
  }
  for (size_t i = 0; i < len; i++) {
    vec[i] = expected[len - i];
  }
  for (size_t i = 0; i < len; i++) {
    TEST(vec[i] == expected[len - i]);
  }

  TEST(!vec.empty());
  for (size_t i = 0; i < len; i++) {
    vec.pop_back();
    TEST(vec.size() == (len - i - 1));
  }
  TEST(vec.empty());

  return true;
}

bool run_test() {
  uint32_t power_shard_sz = 10;

  auto expected_ints = make_int_range_vec(0, 1000);
  ABORT_IF_FAILED(test_push_and_pop<int>(expected_ints, power_shard_sz));

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
