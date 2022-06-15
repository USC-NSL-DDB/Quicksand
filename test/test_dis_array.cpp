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
          +[](ErasedType &, DistributedArray<int> arr, uint32_t arr_sz,
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

bool run_test() {
  uint32_t power_shard_sz = 10;

  auto int_test_data = make_int_range_vec(0, 4000);
  ABORT_IF_FAILED(test_dis_array<int>(int_test_data, power_shard_sz));

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
