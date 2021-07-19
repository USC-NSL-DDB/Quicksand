#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iostream>

extern "C" {
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "nu/commons.hpp"
#include "nu/utils/perf.hpp"

using namespace nu;

constexpr static uint32_t kNumThreads = 100;
constexpr static double kTargetMops = 10;
constexpr static uint32_t kNumSeconds = 5;

class FakeWorkAdapter : public PerfAdapter {
public:
  std::unique_ptr<PerfThreadState> create_thread_state() override {
    return std::make_unique<PerfThreadState>();
  }

  std::unique_ptr<PerfRequest> gen_req(PerfThreadState *state) override {
    return std::make_unique<PerfRequest>();
  }

  void serve_req(PerfThreadState *state, const PerfRequest *perf) override {}
};

namespace nu {

class Test {
public:
  bool run() {
    FakeWorkAdapter fake_work_adapter;
    Perf perf(fake_work_adapter);
    perf.run(kNumThreads, kTargetMops, kNumSeconds * kOneSecond);
    auto real_mops = perf.get_real_mops();
    if (std::abs(real_mops / kTargetMops - 1) > 0.05) {
      return false;
    }
    if (perf.get_nth_lat(99.9) > 20) {
      return false;
    }

    return true;
  }
};

} // namespace nu

int main(int argc, char **argv) {
  int ret;

  if (argc < 2) {
    std::cerr << "usage: [cfg_file]" << std::endl;
    return -EINVAL;
  }

  ret = rt::RuntimeInit(std::string(argv[1]), [] {
    std::cout << "Running " << __FILE__ "..." << std::endl;
    Test test;
    if (test.run()) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}
