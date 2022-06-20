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

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_vector.hpp"
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

#define CHECK_TEST_FAILURE(cond, file, line)       \
  do {                                             \
    if (!(cond)) {                                 \
      printf("TEST FAILURE: %s:%d\n", file, line); \
      return false;                                \
    }                                              \
  } while (0);

#define TEST(cond) CHECK_TEST_FAILURE(cond, __FILE__, __LINE__)

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
    vec.set(i, expected[len - i]);
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

bool test_apply() {
  uint32_t power_shard_sz = 10;
  uint32_t test_data_sz = 1000;

  auto test_strs = make_test_str_vec(test_data_sz);
  auto vec = make_dis_vector<std::string>(power_shard_sz);
  for (uint32_t i = 0; i < vec.size() / 2; i++) {
    vec.apply(
        i, +[](std::string &s) {
          std::transform(s.begin(), s.end(), s.begin(), ::toupper);
        });
  }

  std::vector<Future<void>> futures;
  for (uint32_t i = vec.size() / 2; i < vec.size(); i++) {
    futures.emplace_back(vec.apply_async(
        i, +[](std::string &s) {
          std::transform(s.begin(), s.end(), s.begin(), ::toupper);
        }));
  }
  for (auto &future : futures) {
    future.get();
  }

  for (auto &s : test_strs) {
    std::transform(s.begin(), s.end(), s.begin(), ::toupper);
  }
  for (uint32_t i = 0; i < vec.size(); i++) {
    TEST(vec[i] == test_strs[i]);
  }

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

bool test_resize() {
  int power_shard_sz = 10;
  auto vec = make_dis_vector<int>(power_shard_sz);

  vec.resize(100);
  TEST(vec.size() == 100);
  for (int i = 0; i < 100; i++) {
    TEST(vec[i] == 0);
  }
  vec.resize(150);
  TEST(vec.size() == 150);
  for (int i = 100; i < 150; i++) {
    TEST(vec[i] == 0);
  }
  vec.resize(150);
  TEST(vec.size() == 150);
  vec.resize(0);
  TEST(vec.size() == 0);

  return true;
}

int double_int(int x) { return x * 2; }

bool test_for_all() {
  int power_shard_sz = 10;
  auto vec = make_dis_vector<int>(power_shard_sz);

  for (int i = 0; i < 1000; i++) {
    vec.push_back(i);
  }
  vec.for_all(+[](int x) { return x * 2; });
  for (int i = 0; i < 1000; i++) {
    TEST(vec[i] == i * 2);
  }

  auto vec2 = make_dis_vector<int>(power_shard_sz);
  for (int i = 0; i < 1000; i++) {
    vec2.push_back(i);
  }
  vec2.for_all(double_int).for_all(double_int).for_all(double_int);
  vec2.for_all(
      +[](int x, int mult1, int mult2) { return x * mult1 * mult2; }, 2, 2);
  for (int i = 0; i < 1000; i++) {
    TEST(vec2[i] == i * 32);
  }

  using MapType = std::unordered_map<int, int>;
  auto vec3 = make_dis_vector<MapType>(power_shard_sz);
  vec3.resize(100);
  vec3.for_all(+[](MapType &map) { map[1] = 1; });
  vec3.for_all(
      +[](MapType &map, int key, int val) { map[key] = val; }, 2, 2);
  auto maps = vec3.collect();
  for (auto &map : maps) {
    TEST(map[1] == 1);
    TEST(map[2] == 2);
  }

  return true;
}

bool test_reduction() {
  int power_shard_sz = 10;
  auto vec = make_dis_vector<int>(power_shard_sz);

  for (int i = 0; i < 100000; i++) {
    vec.push_back(1);
  }

  int sum = vec.reduce(
      0, +[](int sum, int x) { return sum + x; });

  TEST(sum == 100000);

  auto strvec = make_dis_vector<std::string>(power_shard_sz);
  for (int i = 0; i < 1000; i++) {
    strvec.push_back("a");
  }
  using WordCountMap = std::unordered_map<std::string, uint32_t>;
  WordCountMap empty_map;
  WordCountMap map = strvec.reduce(
      empty_map, +[](WordCountMap &map, std::string &s) { map[s]++; });
  TEST(map["a"] == 1000);

  return true;
}

bool run_test() {
  ABORT_IF_FAILED(test_push_pop());
  ABORT_IF_FAILED(test_apply());
  ABORT_IF_FAILED(test_vec_clear());
  ABORT_IF_FAILED(test_capacity());
  ABORT_IF_FAILED(test_capacity_reserve());
  ABORT_IF_FAILED(test_resize());
  ABORT_IF_FAILED(test_for_all());
  ABORT_IF_FAILED(test_reduction());

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
