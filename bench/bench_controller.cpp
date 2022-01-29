#include <atomic>
#include <functional>
#include <iostream>
#include <random>
#include <vector>

#include <thread.h>

#include "nu/ctrl_client.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/perf.hpp"

using namespace nu;

constexpr uint32_t kNumThreads = 18;
constexpr uint32_t kTargetMops = 2;
constexpr uint64_t kPerfDurationUs = 10 * kOneSecond;
constexpr uint32_t kNumProclets = 65566;

namespace nu {

struct PerfResolveObjThreadState : PerfThreadState {
  PerfResolveObjThreadState()
      : rd(), gen(rd()), dist_proclet_num(1, kNumProclets) {}

  std::random_device rd;
  std::mt19937 gen;
  std::uniform_int_distribution<> dist_proclet_num;
};

struct PerfResolveObjReq : PerfRequest {
  PerfResolveObjReq(uint32_t num) : proclet_num(num) {}

  uint32_t proclet_num;
};

class PerfResolveObjAdapter : public PerfAdapter {
public:
  PerfResolveObjAdapter(ControllerClient *client) : client_(client) {}

  std::unique_ptr<PerfThreadState> create_thread_state() {
    return std::make_unique<PerfResolveObjThreadState>();
  }

  std::unique_ptr<PerfRequest> gen_req(PerfThreadState *perf_state) {
    auto *state = reinterpret_cast<PerfResolveObjThreadState *>(perf_state);
    auto proclet_num = (state->dist_proclet_num)(state->gen);
    return std::make_unique<PerfResolveObjReq>(proclet_num);
  }

  bool serve_req(PerfThreadState *perf_state, const PerfRequest *perf_req) {
    auto *req = reinterpret_cast<const PerfResolveObjReq *>(perf_req);
    RemObjID proclet_id = kMaxHeapVAddr - req->proclet_num * kHeapSize;
    auto ip = client_->resolve_obj(proclet_id);
    BUG_ON(!ip);
    return true;
  }

private:
  ControllerClient *client_;
};

class PerfGetMigrationDestAdapter : public PerfAdapter {
public:
  PerfGetMigrationDestAdapter(ControllerClient *client) : client_(client) {}

  std::unique_ptr<PerfThreadState> create_thread_state() {
    return std::make_unique<PerfThreadState>();
  }

  std::unique_ptr<PerfRequest> gen_req(PerfThreadState *state) {
    return std::make_unique<PerfRequest>();
  }

  bool serve_req(PerfThreadState *state, const PerfRequest *req) {
    Resource resource{0, 0};
    auto ip = client_->get_migration_dest(resource);
    BUG_ON(!ip);
    return true;
  }

private:
  ControllerClient *client_;
};

struct PerfUpdateLocationThreadState : PerfThreadState {
  PerfUpdateLocationThreadState()
      : rd(), gen(rd()), dist_proclet_num(1, kNumProclets) {}

  std::random_device rd;
  std::mt19937 gen;
  std::uniform_int_distribution<> dist_proclet_num;
};

struct PerfUpdateLocationReq : PerfRequest {
  PerfUpdateLocationReq(uint32_t num) : proclet_num(num) {}

  uint32_t proclet_num;
};

class PerfUpdateLocationAdapter : public PerfAdapter {
public:
  PerfUpdateLocationAdapter(ControllerClient *client) : client_(client) {}

  std::unique_ptr<PerfThreadState> create_thread_state() {
    return std::make_unique<PerfUpdateLocationThreadState>();
  }

  std::unique_ptr<PerfRequest> gen_req(PerfThreadState *perf_state) {
    auto *state = reinterpret_cast<PerfUpdateLocationThreadState *>(perf_state);
    auto proclet_num = (state->dist_proclet_num)(state->gen);
    return std::make_unique<PerfUpdateLocationReq>(proclet_num);
  }

  bool serve_req(PerfThreadState *perf_state, const PerfRequest *perf_req) {
    auto *req = reinterpret_cast<const PerfUpdateLocationReq *>(perf_req);
    RemObjID proclet_id = kMaxHeapVAddr - req->proclet_num * kHeapSize;
    client_->update_location(proclet_id, get_cfg_ip());
    return true;
  }

private:
  ControllerClient *client_;
};
  
class Test {
public:
  void run() {
    {
      PerfResolveObjAdapter perf_resolve_obj_adapter(
          Runtime::controller_client.get());
      Perf perf(perf_resolve_obj_adapter);
      perf.run(kNumThreads, kTargetMops, kPerfDurationUs);
      std::cout << "resolve_obj() mops = " << perf.get_real_mops() << std::endl;
    }

    {
      PerfGetMigrationDestAdapter perf_get_migration_dest_adapter(
          Runtime::controller_client.get());
      Perf perf(perf_get_migration_dest_adapter);
      perf.run(kNumThreads, kTargetMops, kPerfDurationUs);
      std::cout << "get_migration_obj() mops = " << perf.get_real_mops()
                << std::endl;
    }

    {
      PerfUpdateLocationAdapter perf_update_location_adapter(
          Runtime::controller_client.get());
      Perf perf(perf_update_location_adapter);
      perf.run(kNumThreads, kTargetMops, kPerfDurationUs);
      std::cout << "update_location() mops = " << perf.get_real_mops()
                << std::endl;
    }
  }

private:
};

} // namespace nu

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) {
    Test test;
    test.run();
  });
}
