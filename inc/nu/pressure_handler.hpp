#include <climits>
#include <cstddef>
#include <memory>
#include <set>

extern "C" {
#include <runtime/pressure.h>
}
#include <net.h>

#include "nu/migrator.hpp"

namespace nu {

using ResourcePressureInfo = struct resource_pressure_info;

struct AuxHandlerState {
  struct TCPWriteTask {
    MigratorConn conn;
    iovec sgl[5];
  };

  TCPWriteTask task;
  bool task_pending = false;
  bool done = false;
};

class PressureHandler {
public:
  constexpr static uint32_t kNumAuxHandlers =
      Migrator::kTransmitHeapNumThreads - 1;
  constexpr static uint32_t kSortedHeapsUpdateIntervalMs = 50;

  PressureHandler();
  ~PressureHandler();
  void mock_set_pressure(ResourcePressureInfo pressure);
  void wait_aux_tasks();
  void dispatch_aux_task(uint32_t handler_id,
                         AuxHandlerState::TCPWriteTask &task);

private:
  struct HeapInfo {
    HeapHeader *header;
    float val;

    bool operator<(const HeapInfo &o) const { return val < o.val; }
  };

  AuxHandlerState aux_handler_states[kNumAuxHandlers];
  std::shared_ptr<std::set<HeapInfo>> mem_pressure_sorted_heaps_;
  std::shared_ptr<std::set<HeapInfo>> cpu_pressure_sorted_heaps_;
  rt::Thread update_thread_;
  bool done_;

  void register_handlers();
  std::vector<HeapRange> pick_heaps(uint32_t min_num_heaps,
                                    uint32_t min_mem_mbs);
  void update_sorted_heaps();
  static void main_handler(void *unsed);
  static void aux_handler(void *args);
};

} // namespace nu
