#include <atomic>
#include <iostream>

#include "nu/obj_server.hpp"
#include "nu/pressure_handler.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/thread.hpp"
#include "nu/utils/time.hpp"

constexpr uint32_t kMagic = 0x12345678;
constexpr uint32_t ip0 = MAKE_IP_ADDR(18, 18, 1, 2);
constexpr uint32_t ip1 = MAKE_IP_ADDR(18, 18, 1, 5);

namespace nu {

class CalleeObj {
public:
  uint32_t foo() {
    Time::delay(1000 * 1000);
    return kMagic;
  }
};

class CallerObj {
public:
  CallerObj() {}

  uint32_t foo(Proclet<CalleeObj> &&callee_obj) {
    return callee_obj.run(&CalleeObj::foo);
  }
};

class Test {
public:
  bool run_callee_migrated_test() {
    auto caller_obj = make_proclet_pinned_at<CallerObj>(ip0);
    auto callee_obj = make_proclet_at<CalleeObj>(ip1);
    auto future =
        caller_obj.run_async(&CallerObj::foo, std::move(callee_obj));
    delay_us(500 * 1000);
    callee_obj.run(+[](CalleeObj &_) { Test::migrate(); });
    return future.get() == kMagic;
  }

  bool run_caller_migrated_test() {
    auto caller_obj = make_proclet_at<CallerObj>(ip0);
    auto callee_obj = make_proclet_pinned_at<CalleeObj>(ip1);
    auto future = caller_obj.run_async(&CallerObj::foo, std::move(callee_obj));
    delay_us(500 * 1000);
    caller_obj.run(+[](CallerObj &_) { Test::migrate(); });
    return future.get() == kMagic;
  }

  bool run_both_migrated_test() {
    auto caller_obj = make_proclet_at<nu::CallerObj>(ip0);
    auto callee_obj = make_proclet_at<nu::CalleeObj>(ip1);
    auto future = caller_obj.run_async(&CallerObj::foo, std::move(callee_obj));
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
