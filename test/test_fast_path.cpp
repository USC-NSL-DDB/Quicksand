#include <atomic>
#include <iostream>

#include "nu/obj_server.hpp"
#include "nu/pressure_handler.hpp"
#include "nu/rem_obj.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/thread.hpp"
#include "nu/utils/time.hpp"

constexpr uint32_t kMagic = 0x12345678;
constexpr netaddr addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 2),
                          .port = nu::ObjServer::kObjServerPort};

class SubObj {
public:
  uint32_t foo() {
    nu::Time::delay(1000 * 1000);
    return kMagic;
  }
};

namespace nu {
class Test {
public:
  Test() : sub_obj_(RemObj<SubObj>::create_at(addr)) {}

  uint32_t foo() {
    auto future = sub_obj_.run_async(&SubObj::foo);
    nu::Time::delay(500 * 1000);
    migrate();
    return future.get();
  }

  void migrate() {
    ResourcePressureInfo pressure = {.mem_mbs_to_release = 1000,
                                     .num_cores_to_release = 0};
    Runtime::pressure_handler->mock_set_pressure(pressure);
  }

private:
  RemObj<SubObj> sub_obj_;
};
} // namespace nu

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    auto test_obj = nu::RemObj<nu::Test>::create_pinned_at(addr);
    if (test_obj.run(&nu::Test::foo) == kMagic) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });
}
