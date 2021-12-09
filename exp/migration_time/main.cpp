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
#include "nu/rem_obj.hpp"
#include "nu/runtime.hpp"

using namespace nu;

Runtime::Mode mode;

constexpr uint32_t kObjSize = 2097152;
constexpr uint32_t kNumObjs = 128;

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
    delay_ms(1000);
  }
};
} // namespace nu

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) {
    netaddr laddr = {.ip = MAKE_IP_ADDR(18, 18, 1, 2),
                     .port = ObjServer::kObjServerPort};
    netaddr raddr = {.ip = MAKE_IP_ADDR(18, 18, 1, 5),
                     .port = ObjServer::kObjServerPort};
    std::vector<RemObj<Obj>> objs;
    for (uint32_t i = 0; i < kNumObjs; i++) {
      objs.emplace_back(RemObj<Obj>::create_at(laddr));
    }
    auto migrator = RemObj<Test>::create_pinned_at(laddr);
    migrator.run(&Test::migrate);

  retry:
    for (auto &obj : objs) {
      if (obj.run(&Obj::get_ip) != raddr.ip) {
        std::cout << obj.run(&Obj::get_ip) << " " << raddr.ip << std::endl;
        timer_sleep(1000 * 1000);
        goto retry;
      }
    }
  });
}
