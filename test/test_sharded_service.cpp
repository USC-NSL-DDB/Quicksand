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
  int get() { return x; }

  bool empty() const { return false; }
  std::size_t size() const { return 1; }
  void split(Key *mid_k, Counter *latter_half) {}
};

bool run_sharded_service() {
  auto service = make_sharded_service<::Counter>(3);
  int delta = 2;
  auto ret0 =
      service.run(0, &::Counter::add, delta);  // Pass a variable argument.
  if (ret0 != 5) {
    return false;
  }
  auto ret1 = service.run(0, &::Counter::add, 2); // Pass a literal argument.
  if (ret1 != 7) {
    return false;
  }

  return true;
}

bool run_sharded_stateless_service() {
  auto service = make_sharded_stateless_service<::Counter>(3);
  if (service.run(&::Counter::get) != 3) {
    return false;
  }

  return true;
}

bool run_test() {
  return run_sharded_service() && run_sharded_stateless_service();
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
