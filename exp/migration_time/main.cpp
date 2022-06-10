#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

extern "C" {
#include <base/time.h>
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "nu/pressure_handler.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"

using namespace nu;

Runtime::Mode mode;

constexpr uint32_t kObjSize = 16777216;
constexpr uint32_t kNumObjs = 1024;

class Obj {
public:
  uint32_t get_ip() { return get_cfg_ip(); }
private:
  uint8_t bytes[kObjSize];
};

namespace nu {
class Test {
public:
  void migrate() {
    ResourcePressureInfo pressure = {.mem_mbs_to_release = 65535,
                                     .num_cores_to_release = 0};
    Runtime::pressure_handler->mock_set_pressure(pressure);
  }
};
} // namespace nu

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) {
    auto l_ip = MAKE_IP_ADDR(18, 18, 1, 2);
    auto r_ip = MAKE_IP_ADDR(18, 18, 1, 3);
    std::vector<Proclet<Obj>> objs;
    for (uint32_t i = 0; i < kNumObjs; i++) {
      objs.emplace_back(make_proclet_at<Obj>(l_ip));
    }
    auto migrator = make_proclet_pinned_at<Test>(l_ip);
    migrator.run(&Test::migrate);

  retry:
    for (auto &obj : objs) {
      if (obj.run(&Obj::get_ip) != r_ip) {
        std::cout << obj.run(&Obj::get_ip) << " " << r_ip << std::endl;
        timer_sleep(1000 * 1000);
        goto retry;
      }
    }
  });
}
