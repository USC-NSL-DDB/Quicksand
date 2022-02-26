#include <iostream>

#include <sync.h>
#include <thread.h>

#include "nu/commons.hpp"
#include "nu/migrator.hpp"
#include "nu/pressure_handler.hpp"
#include "nu/runtime.hpp"

constexpr static bool kEnableLogging = false;

namespace nu {

PressureHandler::PressureHandler() : done_(false) {
  register_handlers();

  update_thread_ = rt::Thread([&] {
    while (!rt::access_once(done_)) {
      timer_sleep(kSortedHeapsUpdateIntervalMs * kOneMilliSecond);
      update_sorted_heaps();
    }
  });
}

PressureHandler::~PressureHandler() {
  done_ = true;
  barrier();
  update_thread_.Join();
}

uint64_t get_heap_size(HeapHeader *heap_header) {
  auto &slab = heap_header->slab;
  auto size_in_bytes = reinterpret_cast<uint64_t>(slab.get_base()) +
                       slab.get_usage() -
                       reinterpret_cast<uint64_t>(heap_header);
  return size_in_bytes;
}

Utility::Utility(HeapHeader *heap_header) {
  auto heap_size = get_heap_size(heap_header);
  auto stack_size = heap_header->thread_cnt.get() * kStackSize;
  auto size = heap_size + stack_size;
  auto time = kFixedCostUs + (size / (kNetBwGbps / 8.0f) / 1000.0f);

  auto cpu_load = heap_header->cpu_load.get_load();
  heap_header->cpu_load.reset();
  auto cpu_pressure_alleviated = cpu_load;
  auto mem_pressure_alleviated = size;

  cpu_pressure_util = cpu_pressure_alleviated / time;
  mem_pressure_util = mem_pressure_alleviated / time;
}

void PressureHandler::update_sorted_heaps() {
  CPULoad::flush_all();

  auto new_mem_pressure_sorted_heaps = std::make_shared<std::set<HeapInfo>>();
  auto new_cpu_pressure_sorted_heaps = std::make_shared<std::set<HeapInfo>>();
  auto all_heaps = Runtime::heap_manager->get_all_heaps();
  for (auto *heap_base : all_heaps) {
    auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
    heap_header->spin_lock.lock();
    if (unlikely(heap_header->status != kPresent || !heap_header->migratable)) {
      heap_header->spin_lock.unlock();
      continue;
    }
    auto [cpu_pressure_utility, mem_pressure_utility] = Utility(heap_header);
    heap_header->spin_lock.unlock();

    HeapInfo cpu{heap_header, cpu_pressure_utility};
    new_cpu_pressure_sorted_heaps->insert(cpu);
    HeapInfo mem{heap_header, mem_pressure_utility};
    new_mem_pressure_sorted_heaps->insert(mem);
  }

  std::atomic_exchange(&cpu_pressure_sorted_heaps_,
                       new_cpu_pressure_sorted_heaps);
  std::atomic_exchange(&mem_pressure_sorted_heaps_,
                       new_mem_pressure_sorted_heaps);
}

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
      unblock_and_relax();
    }
  }
  store_release(&pressure.status, HANDLED);
}

void PressureHandler::wait_aux_tasks() {
  for (uint32_t i = 0; i < kNumAuxHandlers; i++) {
    while (rt::access_once(aux_handler_states[i].task_pending)) {
      unblock_and_relax();
    }
  }
}

void PressureHandler::init_aux_handler(uint32_t handler_id,
                                       MigratorConn &&conn) {
  aux_handler_states[handler_id].conn = std::move(conn);
}

void PressureHandler::dispatch_aux_tcp_task(
    uint32_t handler_id, std::vector<iovec> &&tcp_write_task) {
  auto &state = aux_handler_states[handler_id];
  while (rt::access_once(state.task_pending)) {
    unblock_and_relax();
  }
  state.tcp_write_task = std::move(tcp_write_task);
  store_release(&state.task_pending, true);
}

void PressureHandler::dispatch_aux_pause_task(uint32_t handler_id) {
  auto &state = aux_handler_states[handler_id];
  while (rt::access_once(state.task_pending)) {
    unblock_and_relax();
  }
  state.pause = true;
  store_release(&state.task_pending, true);
}

void PressureHandler::aux_handler(void *args) {
  auto *state = reinterpret_cast<AuxHandlerState *>(args);
  // Service tasks.
  while (!rt::access_once(state->done)) {
    if (unlikely(load_acquire(&state->task_pending))) {
      if (state->pause) {
        pause_migrating_ths_aux();
        store_release(&state->pause, false);
      } else {
        auto *c = state->conn.get_tcp_conn();
        BUG_ON(c->WritevFull(std::span<const iovec>(state->tcp_write_task),
                             /* nt = */ true,
                             /* poll = */ true) < 0);
      }
      store_release(&state->task_pending, false);
    }
    unblock_and_relax();
  }
  state->conn.release();
  store_release(&state->done, false);
}

std::vector<HeapRange> PressureHandler::pick_heaps(uint32_t min_num_heaps,
                                                   uint32_t min_mem_mbs) {
  float picked_mem_mbs = 0;
  uint32_t picked_num = 0;
  bool done = false;
  std::vector<HeapRange> picked_heaps;

  auto pick_fn = [&](HeapHeader *header) {
    auto size = get_heap_size(header);
    HeapRange range{header, size};
    picked_heaps.push_back(range);
    picked_mem_mbs += size / static_cast<float>(kOneMB);
    picked_num++;
    done = ((picked_mem_mbs >= min_mem_mbs) && (picked_num >= min_num_heaps));
  };

  bool cpu_pressure = min_num_heaps;
  auto sorted_heaps = cpu_pressure
                          ? std::atomic_load(&cpu_pressure_sorted_heaps_)
                          : std::atomic_load(&mem_pressure_sorted_heaps_);
  if (sorted_heaps) {
    auto iter = sorted_heaps->begin();
    while (iter != sorted_heaps->end() && !done) {
      auto *header = iter->header;
      iter = sorted_heaps->erase(iter);
      if (unlikely(header->status != kPresent)) {
        continue;
      }
      pick_fn(header);
    }
  }

  if (unlikely(!done)) {
    picked_heaps.clear();
    picked_num = 0;
    picked_mem_mbs = 0;
    auto all_heaps = Runtime::heap_manager->get_all_heaps();
    auto iter = all_heaps.begin();
    while (iter != all_heaps.end() && !done) {
      auto *header = reinterpret_cast<HeapHeader *>(*iter);
      iter++;
      if (unlikely(header->status != kPresent || !header->migratable)) {
        continue;
      }
      pick_fn(header);
    }
  }

  return picked_heaps;
}

void PressureHandler::mock_set_pressure(ResourcePressureInfo pressure) {
  pressure.status = PENDING;
  *resource_pressure_info = pressure;
}

} // namespace nu
