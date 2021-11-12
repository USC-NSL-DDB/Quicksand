#include <iostream>

#include <sync.h>

#include "nu/migrator.hpp"
#include "nu/pressure_handler.hpp"
#include "nu/runtime.hpp"

constexpr static bool kEnableLogging = false;

namespace nu {

PressureHandler::PressureHandler() { register_handlers(); }

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

  auto *handler = Runtime::pressure_handler.get();
  auto core_ratio =
      static_cast<double>(pressure.num_cores_to_release) /
      (pressure.num_cores_to_release + pressure.num_cores_granted);
  auto min_num_heaps =
      Runtime::heap_manager->get_num_present_heaps() * core_ratio;
  auto min_mem_mbs = pressure.mem_mbs_to_release;
  auto heaps = handler->pick_heaps(min_num_heaps, min_mem_mbs);
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
    rt::access_once(handler->aux_handler_states[i].done) = true;
  }
  // Wait for aux pressure handlers to exit.
  for (uint32_t i = 0; i < PressureHandler::kNumAuxHandlers; i++) {
    while (rt::access_once(handler->aux_handler_states[i].done)) {
      cpu_relax();
    }
  }
  store_release(&pressure.status, HANDLED);
}

void PressureHandler::wait_aux_tasks() {
  for (uint32_t i = 0; i < kNumAuxHandlers - 1; i++) {
    while (rt::access_once(aux_handler_states[i].task_pending)) {
      cpu_relax();
    }
  }
}

void PressureHandler::dispatch_aux_task(uint32_t handler_id,
                                        AuxHandlerState::TCPWriteTask &task) {
  auto &state = aux_handler_states[handler_id];
  state.task = std::move(task);
  store_release(&state.task_pending, true);
}

void PressureHandler::aux_handler(void *args) {
  auto *state = reinterpret_cast<AuxHandlerState *>(args);
  while (!rt::access_once(state->done)) {
    if (unlikely(load_acquire(&state->task_pending))) {
      auto &task = state->task;
      auto *c = task.conn.get_tcp_conn();
      BUG_ON(c->WritevFull(std::span(reinterpret_cast<const iovec *>(task.sgl),
                                     std::size(task.sgl)),
                           /* nt = */ false,
                           /* poll = */ true) < 0);
      rt::access_once(state->task_pending) = false;
      task.conn.release();
    }
    pause_local_migrating_threads();
    cpu_relax();
  }
  state->done = false;
}

std::vector<HeapRange> PressureHandler::pick_heaps(uint32_t min_num_heaps,
                                                   uint32_t min_mem_mbs) {
  std::vector<HeapRange> heaps;
  uint32_t picked_heaps_mem_mbs = 0;

  CPULoad::flush_all();

  auto &all_heaps = Runtime::heap_manager->acquire_heaps_set();
  for (auto *heap_base : all_heaps) {
    auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
    if (heap_header->migratable) {
      auto &slab = heap_header->slab;
      uint64_t len = reinterpret_cast<uint64_t>(slab.get_base()) +
                     slab.get_usage() - reinterpret_cast<uint64_t>(heap_header);
      [[maybe_unused]] auto cpu_load =
          heap_header->cpu_load.get_load(); // Do something with it.
      heap_header->cpu_load.reset();

      HeapRange range{heap_header, len};
      heaps.push_back(range);

      picked_heaps_mem_mbs += len / kOneMB;
      if (picked_heaps_mem_mbs >= min_mem_mbs &&
          heaps.size() >= min_num_heaps) {
        break;
      }
    }
  }
  Runtime::heap_manager->release_heaps_set();

  return heaps;
}

void PressureHandler::mock_set_pressure(ResourcePressureInfo pressure) {
  RuntimeHeapGuard guard;

  pressure.status = PENDING;
  *resource_pressure_info = pressure;
}

} // namespace nu
