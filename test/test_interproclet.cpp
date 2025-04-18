#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>
#include "thread.h"
#include "timer.h"

extern "C" {
#include <base/time.h>
#include <net/ip.h>
}
#include <runtime.h>

#include "nu/pressure_handler.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"

using namespace nu;

constexpr static int kMagic = 0xDEADBEEF;

namespace nu {
class Callee {
 public:
  int say_hello() {
    const int num1 = 1;
    const int num2 = 2;

    std::cout << "Inside callee. Hello!" << std::endl;
    int sum = num1 + num2;
    // ensure the caller triggers migration.
    delay_us(5 * 1000 * 1000);
    std::cout << "Inside callee. Ready to return." << std::endl << std::flush;
    //
    // // Set resource pressure using the mock interface
    // {
    //   rt::Preempt p;
    //   rt::PreemptGuard g(&p);
    //   get_runtime()->pressure_handler()->mock_set_pressure();
    // }
    // // Ensure that the migration happens before the function returns.
    // delay_us(1000 * 1000);
    // Should be printed at the new server node.
    // std::cout << "I am here" << std::endl;
    return sum;
  }
};

class DummyData {
 public:
  int data_;
  DummyData(int data) : data_(data) {}
};

class Caller {
 public:
  int call_callee(Proclet<Callee> callee) {
    std::vector<DummyData> data_vec;
    data_vec.reserve(3); 
    data_vec.emplace_back(1);
    data_vec.emplace_back(2);
    data_vec.emplace_back(3);
    
    rt::Thread([&] {
      // ensure the first request goes out before migration.
      delay_us(2 * 1000 * 1000);
      std::cout << "Inside caller. Triggering migration." << std::endl << std::flush;
      rt::Preempt p;
      rt::PreemptGuard g(&p);
      get_runtime()->pressure_handler()->mock_set_pressure();
    }).Detach();

    std::cout << "Inside caller. Before call callee." << std::endl;
    auto ret = callee.run(&Callee::say_hello);
    std::cout << "Inside caller. Callee returned: " << ret << std::endl << std::flush;
    return ret;
    // // Set resource pressure using the mock interface
    // {
    //   rt::Preempt p;
    //   rt::PreemptGuard g(&p);
    //   get_runtime()->pressure_handler()->mock_set_pressure();
    // }
    // // Ensure that the migration happens before the function returns.
    // delay_us(1000 * 1000);
    // // Should be printed at the new server node.
    // std::cout << "I am here" << std::endl;
    // return kMagic;
  }
};
}  // namespace nu

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) {
    auto caller_ip = MAKE_IP_ADDR(18, 18, 1, 2);
    auto caller = make_proclet<Caller>(false, std::nullopt, caller_ip);

    auto callee_ip = MAKE_IP_ADDR(18, 18, 1, 3);
    auto callee = make_proclet<Callee>(false, std::nullopt, callee_ip);

    bool passed = (caller.run(&Caller::call_callee, callee) == 3);

    if (passed) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });
}
