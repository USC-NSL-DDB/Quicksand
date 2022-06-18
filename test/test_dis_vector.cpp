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

std::random_device rd;
std::mt19937 mt(rd());
std::uniform_int_distribution<int> dist('A', 'z');

std::string random_str(uint32_t len) {
  std::string str = "";
  for (uint32_t i = 0; i < len; i++) {
    str += dist(mt);
  }
  return str;
}

std::vector<std::string> make_test_str_vec(uint32_t size) {
  int test_str_len = 35;
  std::vector<std::string> vec(size);
  for (uint32_t i = 0; i < size; i++) {
    vec[i] = random_str(test_str_len);
  }
  return vec;
}

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

bool test_push_pop() {
  uint32_t power_shard_sz = 10;
  uint32_t test_data_sz = 12345;

  auto expected_ints = make_int_range_vec(0, test_data_sz);
  ABORT_IF_FAILED(test_push_and_pop<int>(expected_ints, power_shard_sz));

  auto test_strs = make_test_str_vec(test_data_sz);
  ABORT_IF_FAILED(test_push_and_pop<std::string>(test_strs, power_shard_sz));

  return true;
}

bool test_vec_clear() {
  auto vec = make_dis_vector<int>(10);

  TEST(vec.empty());
  vec.clear();
  TEST(vec.empty());

  for (size_t i = 0; i < 10; i++) {
    vec.push_back(i);
  }

  TEST(!vec.empty());
  vec.clear();
  TEST(vec.empty());

  return true;
}

bool test_capacity() {
  int power_shard_sz = 10;
  auto vec = make_dis_vector<int>(power_shard_sz);
  TEST(vec.capacity() == 0);
  vec.push_back(2);
  TEST(vec.capacity() > 0);

  for (int i = 0; i < (1 << power_shard_sz); i++) {
    vec.push_back(i);
  }
  size_t cap = vec.capacity();
  vec.clear();
  TEST(vec.capacity() == cap);

  vec.shrink_to_fit();  // non-binding
  TEST(vec.capacity() <= cap);

  return true;
}

bool test_capacity_reserve() {
  int power_shard_sz = 10;
  auto vec = make_dis_vector<int>(power_shard_sz);

  vec.reserve(12345);
  TEST(vec.capacity() >= 12345);
  TEST(vec.size() == 0);
  vec.reserve(22);
  TEST(vec.capacity() >= 12345);

  return true;
}

bool run_test() {
  ABORT_IF_FAILED(test_push_pop());
  ABORT_IF_FAILED(test_vec_clear());
  ABORT_IF_FAILED(test_capacity());
  ABORT_IF_FAILED(test_capacity_reserve());

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
