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
  bool task_ready = false;
  bool done = false;
};

class PressureHandler {
public:
  constexpr static uint32_t kNumAuxHandlers =
      Migrator::kTransmitHeapNumThreads - 1;

  static void register_handlers();
  static void main_handler(void *unsed);
  static void aux_handler(void *args);
  static void mock_set_pressure(ResourcePressureInfo pressure);

  static AuxHandlerState aux_handler_states[kNumAuxHandlers];
};

} // namespace nu
