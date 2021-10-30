#include <algorithm>
#include <cereal/types/utility.hpp>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <utility>
#include <vector>

extern "C" {
#include <net/ip.h>
}
#include <runtime.h>

#include "nu/rem_obj.hpp"
#include "nu/runtime.hpp"

using namespace nu;

Runtime::Mode mode;

class CPULightObj {
public:
  constexpr static uint32_t kTimeUs = 10;

  void compute() { delay_us(kTimeUs); }
};

class CPUHeavyObj {
public:
  constexpr static uint32_t kTimeUs = 100;

  void compute() { delay_us(kTimeUs); }
};

namespace nu {

class Test {
public:
  bool run() {
    bool passed;

    auto light_obj = RemObj<CPULightObj>::create();
    auto heavy_obj = RemObj<CPUHeavyObj>::create();

    for (uint32_t i = 0; i < 100000; i++) {
      auto light_future = light_obj.run_async(&CPULightObj::compute);
      auto heavy_future = heavy_obj.run_async(&CPUHeavyObj::compute);
    }

    auto light_cpu_load = light_obj.run(+[](CPULightObj &_) {
      auto *heap_header = Runtime::get_current_obj_heap_header();
      return heap_header->cpu_load.get_load();
    });

    auto heavy_cpu_load = heavy_obj.run(+[](CPUHeavyObj &_) {
      auto *heap_header = Runtime::get_current_obj_heap_header();
      return heap_header->cpu_load.get_load();
    });

    auto ratio = heavy_cpu_load / light_cpu_load;
    auto expected_ratio = CPUHeavyObj::kTimeUs / CPULightObj::kTimeUs;

    passed = (std::abs(ratio - expected_ratio) / expected_ratio < 0.2);
    return passed;
  };
};
} // namespace nu

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) {
    Test test;
    if (test.run()) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });
}
