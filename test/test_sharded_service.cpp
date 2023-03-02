#include "nu/runtime.hpp"
#include "nu/sharded_service.hpp"

using namespace nu;

bool run_test() {
  auto sharded_service = make_sharded_service();
  auto ret = sharded_service.compute_on(
      1, +[](Service &_, int x) { return x + 1; }, 1);
  return ret == 2;
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
