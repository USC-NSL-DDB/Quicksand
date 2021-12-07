#include <atomic>
#include <iostream>

#include "nu/obj_server.hpp"
#include "nu/pressure_handler.hpp"
#include "nu/rem_obj.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/thread.hpp"
#include "nu/utils/time.hpp"

constexpr uint32_t kMagic = 0x12345678;
constexpr netaddr addr0 = {.ip = MAKE_IP_ADDR(18, 18, 1, 2),
                           .port = nu::ObjServer::kObjServerPort};
constexpr netaddr addr1 = {.ip = MAKE_IP_ADDR(18, 18, 1, 2),
                           .port = nu::ObjServer::kObjServerPort};

namespace nu {

class CalleeObj {
public:
  uint32_t foo() {
    nu::Time::delay(1000 * 1000);
    return kMagic;
  }
};

class CallerObj {
public:
  CallerObj() {}

  uint32_t foo(RemObj<CalleeObj> &&callee_obj) {
    return callee_obj.run(&CalleeObj::foo);
  }
};

class Test {
public:
  bool run_callee_migrated_test() {
    auto caller_obj = nu::RemObj<nu::CallerObj>::create_pinned_at(addr0);
    auto callee_obj = nu::RemObj<nu::CalleeObj>::create_at(addr1);
    auto future =
        caller_obj.run_async(&nu::CallerObj::foo, std::move(callee_obj));
    delay_us(500 * 1000);
    callee_obj.run(+[](CalleeObj &_) { Test::migrate(); });
    return future.get() == kMagic;
  }

  bool run_caller_migrated_test() {
    auto caller_obj = nu::RemObj<nu::CallerObj>::create_at(addr0);
    auto callee_obj = nu::RemObj<nu::CalleeObj>::create_pinned_at(addr1);
    auto future =
        caller_obj.run_async(&nu::CallerObj::foo, std::move(callee_obj));
    delay_us(500 * 1000);
    caller_obj.run(+[](CallerObj &_) { Test::migrate(); });
    return future.get() == kMagic;
  }

  bool run_both_migrated_test() {
    auto caller_obj = nu::RemObj<nu::CallerObj>::create_at(addr0);
    auto callee_obj = nu::RemObj<nu::CalleeObj>::create_at(addr1);
    auto future =
        caller_obj.run_async(&nu::CallerObj::foo, std::move(callee_obj));
    delay_us(500 * 1000);
    caller_obj.run(+[](CallerObj &_) { Test::migrate(); });
    callee_obj.run(+[](CalleeObj &_) { Test::migrate(); });
    return future.get() == kMagic;
  }

  bool run_all_tests() {
    return run_callee_migrated_test() && run_caller_migrated_test() &&
           run_both_migrated_test();
  }

  static void migrate() {
    ResourcePressureInfo pressure = {.mem_mbs_to_release = 1000,
                                     .num_cores_to_release = 0};
    Runtime::pressure_handler->mock_set_pressure(pressure);
  }
};

} // namespace nu

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    nu::Test test;
    if (test.run_all_tests()) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });
}
