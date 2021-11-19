#include <atomic>
#include <iostream>

#include "nu/pressure_handler.hpp"
#include "nu/rem_obj.hpp"
#include "nu/runtime.hpp"
#include "nu/time.hpp"
#include "nu/utils/thread.hpp"

constexpr uint32_t kNumThreads = 1000;

namespace nu {
class Test {
public:
  void inc() {
    nu::Thread t([&] {
      Time::sleep(1000 * 1000);
      s_++;
    });
    t.join();
  }

  int read() { return s_; }

  void migrate() {
    ResourcePressureInfo pressure = {.mem_mbs_to_release = 1000,
                                     .num_cores_to_release = 0};
    Runtime::pressure_handler->mock_set_pressure(pressure);
  }

private:
  std::atomic<int> s_;
};
} // namespace nu

bool run_in_obj_env() {
  auto rem_obj = nu::RemObj<nu::Test>::create();
  std::vector<nu::Future<void>> futures;
  for (uint32_t i = 0; i < kNumThreads; i++) {
    futures.emplace_back(rem_obj.run_async(&nu::Test::inc));
  }
  rem_obj.run(&nu::Test::migrate);
  for (auto &future : futures) {
    future.get();
  }
  return rem_obj.run(&nu::Test::read) == kNumThreads;
}

bool run_in_runtime_env() {
  std::atomic<int> s{0};
  std::vector<nu::Thread> threads;

  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads.emplace_back(nu::Thread([&] { s++; }));
  }  
  for (auto &thread : threads) {
    thread.join();
  }
  return s == kNumThreads;
}

bool run_all_tests() {
  return run_in_runtime_env() && run_in_obj_env();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    if (run_all_tests()) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });
}
