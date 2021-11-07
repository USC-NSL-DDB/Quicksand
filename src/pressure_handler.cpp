#include <iostream>

#include <sync.h>

#include "nu/migrator.hpp"
#include "nu/pressure_handler.hpp"
#include "nu/runtime.hpp"

constexpr static bool kEnableLogging = false;

namespace nu {

AuxHandlerState
    PressureHandler::aux_handler_states[PressureHandler::kNumAuxHandlers];

void PressureHandler::register_handlers() {
  add_resource_pressure_handler(main_handler, nullptr);
  for (uint32_t i = 0; i < kNumAuxHandlers; i++) {
    add_resource_pressure_handler(aux_handler, &aux_handler_states[i]);
  }
}

void PressureHandler::main_handler(void *unused) {
  auto &pressure = *resource_pressure_info;
  if constexpr (kEnableLogging) {
    std::cout << "Detect pressure = { .num_cores = "
              << static_cast<int>(pressure.num_cores_to_release)
              << ", .mem_mbs = "
              << static_cast<int>(pressure.mem_mbs_to_release) << " }."
              << std::endl;
  }

  auto core_ratio =
      static_cast<double>(pressure.num_cores_to_release) /
      (pressure.num_cores_to_release + pressure.num_cores_granted);
  auto min_num_heaps =
      Runtime::heap_manager->get_num_present_heaps() * core_ratio;
  auto min_mem_mbs = pressure.mem_mbs_to_release;
  auto heaps = Runtime::heap_manager->pick_heaps(min_num_heaps, min_mem_mbs);
  if (!heaps.empty()) {
    Resource resource;
    resource.cores = pressure.num_cores_to_release;
    resource.mem_mbs = pressure.mem_mbs_to_release;
    Runtime::migrator->migrate(resource, heaps);
  }

  if constexpr (kEnableLogging) {
    std::cout << "Migrate " << heaps.size() << " heaps." << std::endl;
  }

  // Pause aux pressure handlers.
  for (uint32_t i = 0; i < PressureHandler::kNumAuxHandlers; i++) {
    rt::access_once(PressureHandler::aux_handler_states[i].done) = true;
  }
  // Wait for aux pressure handlers to exit.
  for (uint32_t i = 0; i < PressureHandler::kNumAuxHandlers; i++) {
    while (rt::access_once(PressureHandler::aux_handler_states[i].done)) {
      cpu_relax();
    }
  }
  store_release(&pressure.status, HANDLED);
}

void PressureHandler::aux_handler(void *args) {
  auto *state = reinterpret_cast<AuxHandlerState *>(args);
  while (!rt::access_once(state->done)) {
    if (unlikely(rt::access_once(state->task_ready))) {
      auto &task = state->task;
      auto *c = task.conn.get_tcp_conn();
      BUG_ON(c->WritevFull(std::span(reinterpret_cast<const iovec *>(task.sgl),
                                     std::size(task.sgl)),
                           /* nt = */ false,
                           /* poll = */ true) < 0);
      rt::access_once(state->task_ready) = false;
      task.conn.release();
    }
    pause_local_migrating_threads();
    cpu_relax();
  }
  state->done = false;
}

void PressureHandler::mock_set_pressure(ResourcePressureInfo pressure) {
  RuntimeHeapGuard guard;

  pressure.status = PENDING;
  *resource_pressure_info = pressure;
}

} // namespace nu
