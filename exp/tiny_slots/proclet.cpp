#include <vector>

#include "runtime.h"

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"

class Obj {
 public:
  void run() {
    while (1)
      ;
  }
};

void do_work() {
  auto num_proclets = rt::RuntimeMaxCores();
  std::vector<nu::Proclet<Obj>> proclets;
  std::vector<nu::Future<void>> futures;
  proclets.reserve(num_proclets);

  for (uint32_t i = 0; i < num_proclets; i++) {
    proclets.push_back(nu::make_proclet<Obj>());
    futures.push_back(proclets.back().run_async(&Obj::run));
  }
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
