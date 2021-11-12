#include <cstddef>

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

  PressureHandler();
  void mock_set_pressure(ResourcePressureInfo pressure);
  void wait_aux_tasks();
  void dispatch_aux_task(uint32_t handler_id,
                         AuxHandlerState::TCPWriteTask &task);

private:
  AuxHandlerState aux_handler_states[kNumAuxHandlers];

  void register_handlers();
  std::vector<HeapRange> pick_heaps(uint32_t min_num_heaps,
                                    uint32_t min_mem_mbs);
  static void main_handler(void *unsed);
  static void aux_handler(void *args);
};

} // namespace nu
