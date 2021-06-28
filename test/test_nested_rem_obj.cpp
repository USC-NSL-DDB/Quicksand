#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "rem_obj.hpp"
#include "rem_raw_ptr.hpp"
#include "runtime.hpp"

using namespace nu;

Runtime::Mode mode;

class Obj {};

void do_work() {
  bool passed = true;

  constexpr auto kMagic = 0xDEADBEEF;
  auto rem_obj_0 = RemObj<Obj>::create();
  auto magic = rem_obj_0.run(+[](Obj &) {
    auto rem_obj_1 = RemObj<Obj>::create();
    return rem_obj_1.run(+[](Obj &) {
      auto rem_obj_2 = RemObj<Obj>::create();
      return rem_obj_2.run(+[](Obj &) {
        auto rem_obj_3 = RemObj<Obj>::create();
        return rem_obj_3.run(+[](Obj &) {
          auto rem_obj_4 = RemObj<Obj>::create();
          return rem_obj_4.run(+[](Obj &) { return kMagic; });
        });
      });
    });
  });
  passed = (magic == kMagic);

  if (passed) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
