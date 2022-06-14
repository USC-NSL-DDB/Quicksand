#include <cereal/types/set.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/unordered_map.hpp>
#include <cereal/types/utility.hpp>
#include <cstdint>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
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

bool run_test() {
  uint32_t power_shard_sz = 10;
  uint32_t arr_sz = 4000;
  DistributedArray<int> arr = make_dis_array<int>(arr_sz, power_shard_sz);

  for (uint32_t i = 0; i < arr_sz; i++) {
    arr.set(i, i);
  }

  auto proclet = make_proclet<ErasedType>();
  if (!proclet.run(
          +[](ErasedType &, DistributedArray<int> arr, uint32_t arr_sz) {
            for (uint32_t i = 0; i < arr_sz; i++) {
              if (arr[i] != (int)i) {
                return false;
              }
            }
            return true;
          },
          arr, arr_sz)) {
    return false;
  }

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
