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

class ComputationalLightObj {
public:
  constexpr static uint32_t kTimeUs = 100;

  void compute() { delay_us(kTimeUs); }
};

class ComputationalHeavyObj {
public:
  constexpr static uint32_t kTimeUs = 1000;

  void compute() { delay_us(kTimeUs); }
};

namespace nu {

class Test {
public:
  bool run() {
    bool passed;

    auto light_obj = RemObj<ComputationalLightObj>::create();
    auto heavy_obj = RemObj<ComputationalHeavyObj>::create();

    for (uint32_t i = 0; i < 10000; i++) {
      auto light_future = light_obj.run_async(&ComputationalLightObj::compute);
      auto heavy_future = heavy_obj.run_async(&ComputationalHeavyObj::compute);
    }

    auto light_compute_intensity = light_obj.run(+[](ComputationalLightObj &_) {
      auto *heap_header = Runtime::get_current_obj_heap_header();
      return heap_header->compute_intensity.get_compute_intensity();
    });

    auto heavy_compute_intensity = heavy_obj.run(+[](ComputationalHeavyObj &_) {
      auto *heap_header = Runtime::get_current_obj_heap_header();
      return heap_header->compute_intensity.get_compute_intensity();
    });

    auto ratio = heavy_compute_intensity / light_compute_intensity;
    auto expected_ratio =
        ComputationalHeavyObj::kTimeUs / ComputationalLightObj::kTimeUs;

    passed = (std::abs(ratio - expected_ratio) / expected_ratio < 0.1);
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
