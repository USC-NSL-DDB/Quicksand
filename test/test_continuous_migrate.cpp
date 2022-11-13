#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <random>
#include <ranges>
#include <tuple>

#include "nu/pressure_handler.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/time.hpp"

using namespace nu;

constexpr static uint32_t kNumProclets = 8192;
constexpr static uint32_t kConcurrency = 1;
constexpr static uint32_t kProcletHeapSize = 2 << 20;
constexpr static uint32_t kMinComputeTimeUs = 10;
constexpr static uint32_t kMaxComputeTimeUs = 1000;
constexpr static uint32_t kArgsSize = 100;

struct Args {
  uint8_t data[kArgsSize];
};

class Obj {
public:
 Obj() : rd_(), gen_(rd_()), dis_(kMinComputeTimeUs, kMaxComputeTimeUs) {}

 Args compute(Args args) {
   for (auto &byte : args.data) {
     ++byte;
   }
   Time::delay(dis_(gen_));
   return args;
 }

private:
 std::random_device rd_;
 std::mt19937 gen_;
 std::uniform_real_distribution<> dis_;
 std::byte data_[kProcletHeapSize];
};

void do_work() {
  std::vector<Proclet<Obj>> proclets;
  for (uint32_t i = 0; i < kNumProclets; i++) {
    proclets.emplace_back(nu::make_proclet<Obj>());
  }

  for (uint32_t i = 0; i < kConcurrency; i++) {
    rt::Spawn([&] {
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_real_distribution<> dis(0, proclets.size());

      Args args;
      std::ranges::fill(args.data, 0);

      while (true) {
        auto &proclet = proclets[dis(gen)];
        auto new_args = proclet.run(&Obj::compute, args);

        for (auto pair : std::views::zip(new_args.data, args.data)) {
          BUG_ON(std::get<0>(pair) != std::get<1>(pair) + 1);
        }
	if (proclet == proclets[0]) {
          proclet.run(+[](Obj &_) {
            Caladan::PreemptGuard g;
            get_runtime()->pressure_handler()->mock_set_pressure();
          });
        }
      }
    });
  }

  get_runtime()->caladan()->thread_park();
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
