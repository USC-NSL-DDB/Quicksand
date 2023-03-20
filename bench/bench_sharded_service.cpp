#include <iostream>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sharded_service.hpp"
#include "nu/utils/thread.hpp"

struct Obj {
  using Key = uint64_t;

  void work(int us) {
    delay_us(us);
  }

  bool empty() const { return false; }
  std::size_t size() const { return 1; }
  void split(Key *mid_k, Obj *latter_half) {}
};

void bench() {
  auto service = nu::make_sharded_stateless_service<Obj>();
  std::vector<nu::Thread> threads;

  for (uint32_t i = 0; i < 40; i++) {
    threads.emplace_back([service]() mutable {
      while (1) {
	service.run(&Obj::work, 500);
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv,
                               [](int, char **) { bench(); });
}
