#include <climits>
#include <cstddef>
#include <memory>
#include <set>

#include <net.h>

#include "nu/migrator.hpp"

namespace nu {

struct AuxHandlerState {
  MigratorConn conn;
  std::vector<iovec> tcp_write_task;
  bool pause = false;
  bool task_pending = false;
  bool done = false;
};

struct Utility {
  Utility();
  Utility(ProcletHeader *proclet_header);

  constexpr static uint32_t kFixedCostUs = 25;
  constexpr static uint32_t kNetBwGbps = 100;
  ProcletHeader *header;
  float mem_pressure_util;
  float cpu_pressure_util;
  uint64_t mem_size;
  float cpu_load;
};

class PressureHandler {
 public:
  constexpr static uint32_t kNumAuxHandlers =
      Migrator::kTransmitProcletNumThreads - 1;
  constexpr static uint32_t kSortedProcletsUpdateIntervalMs = 50;
  constexpr static uint32_t kHandlerSleepUs = 100;

  PressureHandler();
  ~PressureHandler();
  void wait_aux_tasks();
  void init_aux_handler(uint32_t handler_id, MigratorConn &&conn);
  void dispatch_aux_tcp_task(uint32_t handler_id,
                             std::vector<iovec> &&tcp_write_task);
  void dispatch_aux_pause_task(uint32_t handler_id);
  void mock_set_pressure();
  bool has_pressure();

 private:
  constexpr static auto kCmpMemUtil = [](Utility x, Utility y) {
    return x.mem_pressure_util > y.mem_pressure_util;
  };
  constexpr static auto kCmpCpuUtil = [](Utility x, Utility y) {
    return x.cpu_pressure_util > y.cpu_pressure_util;
  };
  std::shared_ptr<std::multiset<Utility, decltype(kCmpMemUtil)>>
      mem_pressure_sorted_proclets_;
  std::shared_ptr<std::multiset<Utility, decltype(kCmpCpuUtil)>>
      cpu_pressure_sorted_proclets_;
  rt::Thread update_th_;
  rt::Thread main_handler_th_;
  rt::Thread aux_handler_ths_[kNumAuxHandlers];
  AuxHandlerState aux_handler_states_[kNumAuxHandlers];
  bool mock_;
  bool done_;

  std::pair<std::vector<ProcletHeader *>, Resource> pick_proclets(
      uint32_t min_num_proclets, uint32_t min_mem_mbs);
  void update_sorted_proclets();
  void main_handler();
  void aux_handler(AuxHandlerState *state);
};

}  // namespace nu

#include "nu/impl/pressure_handler.ipp"
