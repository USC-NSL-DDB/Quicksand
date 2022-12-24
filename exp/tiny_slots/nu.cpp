#include "nu/resource_reporter.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/time.hpp"

void do_work() {
  while (1) {
    nu::Time::sleep(nu::kOneSecond);
    auto free_resources =
        nu::get_runtime()->resource_reporter()->get_global_free_resources();
    std::cout << "******************" << std::endl;
    for (auto &[_, resource] : free_resources) {
      std::cout << resource.cores << " " << resource.mem_mbs << std::endl;
    }
  }
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
