#include <iostream>

#include <sync.h>
#include <thread.h>

#include "nu/migrator.hpp"
#include "nu/pressure_handler.hpp"
#include "nu/runtime.hpp"

constexpr static bool kEnableLogging = false;

namespace nu {

PressureHandler::PressureHandler() {
  register_handlers();
  update_thread_ = rt::Thread([&] {
    done_ = false;
    while (!rt::access_once(done_)) {
      timer_sleep(kSortedHeapsUpdateIntervalMs * kOneMilliSecond);
      update_sorted_heaps();
    }
  });
}

PressureHandler::~PressureHandler() {
  rt::access_once(done_) = true;
  update_thread_.Join();
}

uint64_t get_heap_size(HeapHeader *heap_header) {
  auto &slab = heap_header->slab;
  auto size_in_bytes = reinterpret_cast<uint64_t>(slab.get_base()) +
                       slab.get_usage() -
                       reinterpret_cast<uint64_t>(heap_header);
  return size_in_bytes;
}

std::pair<float, float> utility(HeapHeader *heap_header) {
  auto size = get_heap_size(heap_header);
  auto cpu_load = heap_header->cpu_load.get_load();
  heap_header->cpu_load.reset();
  cpu_load = std::max(cpu_load, static_cast<float>(1e-5));
  auto cpu_pressure_utility = size / cpu_load;
  auto mem_pressure_utility = size * cpu_load;
  return std::make_pair(cpu_pressure_utility, mem_pressure_utility);
}

void PressureHandler::update_sorted_heaps() {
  CPULoad::flush_all();

  auto new_mem_pressure_sorted_heaps = std::make_shared<std::set<HeapInfo>>();
  auto new_cpu_pressure_sorted_heaps = std::make_shared<std::set<HeapInfo>>();
  auto all_heaps = Runtime::heap_manager->get_all_heaps();
  for (auto *heap_base : all_heaps) {
    auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
    heap_header->spin_lock.lock();
    if (unlikely(!heap_header->present || !heap_header->migratable)) {
      heap_header->spin_lock.unlock();
      continue;
    }
    auto [cpu_pressure_utility, mem_pressure_utility] = utility(heap_header);
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
      cpu_relax();
    }
  }
  store_release(&pressure.status, HANDLED);
}

void PressureHandler::wait_aux_tasks() {
  for (uint32_t i = 0; i < kNumAuxHandlers; i++) {
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
    prioritize_local_rcu_readers();
    cpu_relax();
  }
  state->done = false;
}

std::vector<HeapRange> PressureHandler::pick_heaps(uint32_t min_num_heaps,
                                                   uint32_t min_mem_mbs) {
  uint32_t picked_mem_mbs = 0;
  uint32_t picked_num = 0;
  bool done = false;
  std::vector<HeapRange> picked_heaps;

  auto pick_fn = [&](HeapHeader *header) {
    auto size = get_heap_size(header);
    HeapRange range{header, size};
    picked_heaps.push_back(range);
    picked_mem_mbs += size / kOneMB;
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
      if (unlikely(!header->present)) {
        continue;
      }
      pick_fn(header);
    }
  }

  if (unlikely(picked_heaps.empty())) {
    auto &all_heaps = Runtime::heap_manager->acquire_all_heaps();
    auto iter = all_heaps.begin();
    while (iter != all_heaps.end() && !done) {
      auto *header = reinterpret_cast<HeapHeader *>(*iter);
      iter++;
      if (unlikely(!header->present || !header->migratable)) {
        continue;
      }
      pick_fn(header);
    }
    Runtime::heap_manager->release_all_heaps();
  }

  return picked_heaps;
}

void PressureHandler::mock_set_pressure(ResourcePressureInfo pressure) {
  RuntimeHeapGuard guard;

  pressure.status = PENDING;
  *resource_pressure_info = pressure;
}

} // namespace nu
