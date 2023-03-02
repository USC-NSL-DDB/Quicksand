#include "nu/runtime.hpp"
#include "nu/sharded_service.hpp"

using namespace nu;

struct Counter {
  using Key = uint64_t;

  Counter() = default;
  Counter(int n) : x(n) {}

  int x;
  int add(int y) {
    x += y;
    return x;
  }
};

bool run_test() {
  auto sharded_service = make_sharded_service<::Counter>(3);
  auto ret0 = sharded_service.run(0, &::Counter::add, 2);
  if (ret0 != 5) {
    return false;
  }
  auto ret1 = sharded_service.run(0, &::Counter::add, 2);
  return ret1 == 7;
}

void do_work() {
  if (run_test()) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
