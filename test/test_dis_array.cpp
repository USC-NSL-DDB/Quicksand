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

#include "nu/dis_array.hpp"
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

template <typename T>
bool test_dis_array(std::vector<T> expected, uint32_t power_shard_sz) {
  uint32_t arr_sz = expected.size();
  auto arr = make_dis_array<T>(arr_sz, power_shard_sz);

  for (uint32_t i = 0; i < arr_sz; i++) {
    arr.set(i, expected[i]);
  }

  for (uint32_t i = 0; i < arr_sz; i++) {
    if (arr[i] != expected[i]) {
      return false;
    }
  }

  auto proclet = make_proclet<ErasedType>();
  if (!proclet.run(
          +[](ErasedType &, DistributedArray<T> arr, uint32_t arr_sz,
              std::vector<T> expected) {
            for (uint32_t i = 0; i < arr_sz; i++) {
              if (arr[i] != expected[i]) {
                return false;
              }
            }
            return true;
          },
          arr, arr_sz, expected)) {
    return false;
  }

  return true;
}

std::vector<int> make_int_range_vec(int start_incl, int end_excl) {
  BUG_ON(start_incl > end_excl);
  std::vector<int> vec(end_excl - start_incl);
  for (int i = 0; i < end_excl - start_incl; i++) {
    vec[i] = i + start_incl;
  }
  return vec;
}

std::vector<std::string> make_test_str_vec(uint32_t size) {
  int test_str_len = 35;
  std::vector<std::string> vec(size);
  for (uint32_t i = 0; i < size; i++) {
    vec[i] = random_str(test_str_len);
  }
  return vec;
}

std::vector<std::vector<std::string>> make_nested_str_vec(uint32_t size) {
  uint32_t sz = sqrt(size);
  std::vector<std::vector<std::string>> vec(sz);
  for (uint32_t i = 0; i < sz; i++) {
    vec[i] = make_test_str_vec(sz);
  }
  return vec;
}

std::vector<std::unordered_map<int, std::string>> make_int_to_str_maps(
    uint32_t size, uint32_t map_entries) {
  using Map = std::unordered_map<int, std::string>;
  std::vector<Map> vec(size);
  for (uint32_t i = 0; i < size; i++) {
    Map map;
    for (uint32_t k = 0; k < map_entries; k++) {
      map[k] = random_str(12);
    }
    vec[i] = map;
  }
  return vec;
}

bool run_test() {
  uint32_t power_shard_sz = 10;
  uint32_t test_arr_sz = 14243;

  auto int_test_data = make_int_range_vec(0, test_arr_sz);
  ABORT_IF_FAILED(test_dis_array<int>(int_test_data, power_shard_sz));

  auto str_test_data = make_test_str_vec(test_arr_sz);
  ABORT_IF_FAILED(test_dis_array<std::string>(str_test_data, power_shard_sz));

  auto str_vecs = make_nested_str_vec(test_arr_sz);
  ABORT_IF_FAILED(
      test_dis_array<std::vector<std::string>>(str_vecs, power_shard_sz));

  using Map = std::unordered_map<int, std::string>;
  auto maps = make_int_to_str_maps(10, 10);
  ABORT_IF_FAILED(test_dis_array<Map>(maps, power_shard_sz));

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
