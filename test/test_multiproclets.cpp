#include <cstdint>
#include <iostream>
#include <optional>

extern "C" {
#include <base/time.h>
#include <net/ip.h>
}
#include <runtime.h>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"

using namespace nu;

constexpr static int kMagic = 0xDEADBEEF;

namespace nu {
class Dummy{
 public:
  Dummy(uint64_t id) : id_(id) {}

  int say_hello() {
    std::cout << "Hello from proclet " << id_ << std::endl;
    while (true) {
      delay_us(1000 * 1000);
    }
    return kMagic;
  }

 private:
  uint64_t id_;
};
}  // namespace nu

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) {
    NodeIP ip1 = MAKE_IP_ADDR(18, 18, 1, 2);
    std::tuple<int> args1(1);
    auto proclet1 = make_proclet<Dummy>(args1, false, std::nullopt, ip1);
    
    NodeIP ip2 = MAKE_IP_ADDR(18, 18, 1, 3);
    std::tuple<int> args2(2);
    auto proclet2 = make_proclet<Dummy>(args2, false, std::nullopt, ip2);
    
    auto fut1 = proclet1.run_async(&Dummy::say_hello);
    auto fut2 = proclet2.run_async(&Dummy::say_hello);
    
    auto r1 = fut1.get();
    auto r2 = fut2.get();
    
    bool passed = (r1 == kMagic && r2 == kMagic);

    if (passed) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });
}
