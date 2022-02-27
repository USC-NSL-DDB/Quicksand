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
  MigratorConn conn;
  std::vector<iovec> tcp_write_task;
  bool pause = false;
  bool task_pending = false;
  bool done = false;
};

struct Utility {
  constexpr static uint32_t kFixedCostUs = 25;
  constexpr static uint32_t kNetBwGbps = 100;

  float cpu_pressure_util;
  float mem_pressure_util;

  Utility(HeapHeader *heap_header);
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
  void init_aux_handler(uint32_t handler_id, MigratorConn &&conn);
  void dispatch_aux_tcp_task(uint32_t handler_id,
                             std::vector<iovec> &&tcp_write_task);
  void dispatch_aux_pause_task(uint32_t handler_id);

private:
  struct HeapInfo {
    HeapHeader *header;
    float val;

    bool operator<(const HeapInfo &o) const {
      if (val == o.val) {
        return header > o.header;
      }
      return val > o.val;
    }
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
