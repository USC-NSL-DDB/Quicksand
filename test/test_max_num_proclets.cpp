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

class Obj {
public:
  Obj(uint32_t x) : x_(x) {}
  uint32_t get() { return x_; }

private:
  uint32_t x_;
};

void do_work() {
  bool passed = true;

  std::vector<RemObj<Obj>> objs;
  for (uint32_t i = 0; i < kMaxNumHeaps; i++) {
    objs.emplace_back(RemObj<Obj>::create(i));
  }
  for (uint32_t i = 0; i < kMaxNumHeaps; i++) {
    auto &obj = objs[i];
    if (obj.run(&Obj::get) != i) {
      passed = false;
      break;
    }
  }

  if (passed) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
