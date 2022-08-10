#include "nu/pressure_handler.hpp"

#include <sync.h>
#include <thread.h>

#include <iostream>

#include "nu/commons.hpp"
#include "nu/migrator.hpp"
#include "nu/runtime.hpp"

constexpr static bool kEnableLogging = false;

namespace nu {

PressureHandler::PressureHandler() : done_(false) {
  register_handlers();

  update_thread_ = rt::Thread([&] {
    while (!rt::access_once(done_)) {
      timer_sleep(kSortedProcletsUpdateIntervalMs * kOneMilliSecond);
      update_sorted_proclets();
    }
  });
}

PressureHandler::~PressureHandler() {
  done_ = true;
  barrier();
  update_thread_.Join();
}

uint64_t get_proclet_size(ProcletHeader *proclet_header) {
  auto &slab = proclet_header->slab;
  auto size_in_bytes = reinterpret_cast<uint64_t>(slab.get_base()) +
                       slab.get_usage() -
                       reinterpret_cast<uint64_t>(proclet_header);
  return size_in_bytes;
}

Utility::Utility(ProcletHeader *proclet_header) {
  auto proclet_size = get_proclet_size(proclet_header);
  auto stack_size = proclet_header->thread_cnt.get() * kStackSize;
  auto size = proclet_size + stack_size;
  auto time = kFixedCostUs + (size / (kNetBwGbps / 8.0f) / 1000.0f);

  auto cpu_load = proclet_header->cpu_load.get_load();
  proclet_header->cpu_load.reset();
  auto cpu_pressure_alleviated = cpu_load;
  auto mem_pressure_alleviated = size;

  cpu_pressure_util = cpu_pressure_alleviated / time;
  mem_pressure_util = mem_pressure_alleviated / time;
}

void PressureHandler::update_sorted_proclets() {
  CPULoad::flush_all();

  auto new_mem_pressure_sorted_proclets =
      std::make_shared<std::set<ProcletInfo>>();
  auto new_cpu_pressure_sorted_proclets =
      std::make_shared<std::set<ProcletInfo>>();
  auto all_proclets = Runtime::proclet_manager->get_all_proclets();
  for (auto *proclet_base : all_proclets) {
    auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);
    proclet_header->spin_lock.lock();
    if (unlikely(proclet_header->status != kPresent ||
                 !proclet_header->migratable)) {
      proclet_header->spin_lock.unlock();
      continue;
    }
    auto [cpu_pressure_utility, mem_pressure_utility] = Utility(proclet_header);
    proclet_header->spin_lock.unlock();

    ProcletInfo cpu{proclet_header, cpu_pressure_utility};
    new_cpu_pressure_sorted_proclets->insert(cpu);
    ProcletInfo mem{proclet_header, mem_pressure_utility};
    new_mem_pressure_sorted_proclets->insert(mem);
  }

  std::atomic_exchange(&cpu_pressure_sorted_proclets_,
                       new_cpu_pressure_sorted_proclets);
  std::atomic_exchange(&mem_pressure_sorted_proclets_,
                       new_mem_pressure_sorted_proclets);
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
  auto min_num_proclets =
      Runtime::proclet_manager->get_num_present_proclets() * core_ratio;
  auto min_mem_mbs = pressure.mem_mbs_to_release;
  auto proclets = handler->pick_proclets(min_num_proclets, min_mem_mbs);
  if (!proclets.empty()) {
    Resource resource;
    resource.cores = pressure.num_cores_to_release;
    resource.mem_mbs = pressure.mem_mbs_to_release;
    Runtime::migrator->migrate(resource, proclets);
  }

  if constexpr (kEnableLogging) {
    std::cout << "Migrate " << proclets.size() << " proclets." << std::endl;
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
  // Serve tasks.
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

std::vector<ProcletRange> PressureHandler::pick_proclets(
    uint32_t min_num_proclets, uint32_t min_mem_mbs) {
  float picked_mem_mbs = 0;
  uint32_t picked_num = 0;
  bool done = false;
  std::set<ProcletRange> picked_proclets;

  auto pick_fn = [&](ProcletHeader *header) {
    auto size = get_proclet_size(header);
    ProcletRange range{header, size};
    picked_proclets.emplace(range);
    picked_mem_mbs += size / static_cast<float>(kOneMB);
    picked_num++;
    done =
        ((picked_mem_mbs >= min_mem_mbs) && (picked_num >= min_num_proclets));
  };

  bool cpu_pressure = min_num_proclets;
  auto sorted_proclets = cpu_pressure
                             ? std::atomic_load(&cpu_pressure_sorted_proclets_)
                             : std::atomic_load(&mem_pressure_sorted_proclets_);
  if (sorted_proclets) {
    auto iter = sorted_proclets->begin();
    while (iter != sorted_proclets->end() && !done) {
      auto *header = iter->header;
      iter = sorted_proclets->erase(iter);
      if (unlikely(header->status != kPresent)) {
        continue;
      }
      pick_fn(header);
    }
  }

  if (unlikely(!done)) {
    auto all_proclets = Runtime::proclet_manager->get_all_proclets();
    if (likely(picked_proclets.size() < all_proclets.size())) {
      picked_proclets.clear();
      picked_num = 0;
      picked_mem_mbs = 0;
      auto iter = all_proclets.begin();
      while (iter != all_proclets.end() && !done) {
        auto *header = reinterpret_cast<ProcletHeader *>(*iter);
        iter++;
        if (unlikely(header->status != kPresent || !header->migratable)) {
          continue;
        }
        pick_fn(header);
      }
    }
  }

  return std::vector<ProcletRange>(picked_proclets.begin(),
                                   picked_proclets.end());
}

void PressureHandler::mock_set_pressure(ResourcePressureInfo pressure) {
  pressure.status = PENDING;
  *resource_pressure_info = pressure;
}

}  // namespace nu
