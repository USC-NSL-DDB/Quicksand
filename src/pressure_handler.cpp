#include <iostream>
#include <limits>
#include <type_traits>

#include <sync.h>
#include <thread.h>

#include "nu/commons.hpp"
#include "nu/migrator.hpp"
#include "nu/runtime.hpp"
#include "nu/pressure_handler.hpp"

constexpr static bool kEnableLogging = false;

namespace nu {

PressureHandler::PressureHandler() : mock_(false), done_(false) {
  register_handlers();

  update_th_ = rt::Thread([&] {
    while (!rt::access_once(done_)) {
      timer_sleep_hp(kSortedProcletsUpdateIntervalMs * kOneMilliSecond);
      update_sorted_proclets();
    }
  });
}

PressureHandler::~PressureHandler() {
  done_ = true;
  barrier();
  update_th_.Join();
}

Utility::Utility() {}

Utility::Utility(ProcletHeader *proclet_header) {
  header = proclet_header;

  auto proclet_size = proclet_header->size();
  auto stack_size = proclet_header->thread_cnt.get() * kStackSize;
  mem_size = proclet_size + stack_size;
  auto time = kFixedCostUs + (mem_size / (kNetBwGbps / 8.0f) / 1000.0f);

  cpu_load = proclet_header->cpu_load.get_load();

  cpu_pressure_util = cpu_load / time;
  mem_pressure_util = mem_size / time;
}

void PressureHandler::update_sorted_proclets() {
  CPULoad::flush_all();

  auto new_mem_pressure_sorted_proclets =
      std::make_shared<decltype(mem_pressure_sorted_proclets_)::element_type>();
  auto new_cpu_pressure_sorted_proclets =
      std::make_shared<decltype(cpu_pressure_sorted_proclets_)::element_type>();
  auto all_proclets = Runtime::proclet_manager->get_all_proclets();
  for (auto *proclet_base : all_proclets) {
    auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);
    Utility u;
    {
      NonBlockingMigrationDisabledGuard guard(proclet_header);
      if (unlikely(!guard || !proclet_header->migratable)) {
        continue;
      }
      u = Utility(proclet_header);
    }
    new_cpu_pressure_sorted_proclets->insert(u);
    new_mem_pressure_sorted_proclets->insert(u);
  }

  std::atomic_exchange(&cpu_pressure_sorted_proclets_,
                       new_cpu_pressure_sorted_proclets);
  std::atomic_exchange(&mem_pressure_sorted_proclets_,
                       new_mem_pressure_sorted_proclets);
}

void PressureHandler::register_handlers() {
  add_resource_pressure_handler(main_handler, nullptr);
  for (uint32_t i = 0; i < kNumAuxHandlers; i++) {
    add_resource_pressure_handler(aux_handler, &aux_handler_states_[i]);
  }
}

void PressureHandler::main_handler(void *unused) {
  auto *handler = Runtime::pressure_handler.get();
  handler->__main_handler();
}

void PressureHandler::__main_handler() {
  while (has_pressure()) {
    if constexpr (kEnableLogging) {
      std::cout << "Detect pressure = { .mem_mbs = "
                << rt::RuntimeToReleaseMemMbs()
                << ", .cpu_pressure = " << rt::RuntimeCpuPressure() << " }."
                << std::endl;
    }

    auto min_num_proclets =
        rt::RuntimeCpuPressure()
            ? Runtime::migrator->get_max_num_proclets_per_migration()
            : 0;
    auto min_mem_mbs = rt::RuntimeToReleaseMemMbs();
    auto [tasks, resource] = pick_tasks(min_num_proclets, min_mem_mbs);
    if (likely(!tasks.empty())) {
      auto num_migrated = Runtime::migrator->migrate(resource, tasks);
      if constexpr (kEnableLogging) {
        std::cout << "Migrate " << num_migrated << " proclets." << std::endl;
      }
      if (unlikely(num_migrated != tasks.size())) {
        break;
      }
    } else {
      if (mock_)  {
        mock_clear_pressure();
      }
      break;
    }
  }

  pause_aux_handlers();

  // Tell iokernel that the pressure has been handled.
  auto &pressure = *resource_pressure_info;
  pressure.mock = false;
  store_release(&pressure.status, HANDLED);
}

void PressureHandler::pause_aux_handlers() {
  // Pause aux pressure handlers.
  for (uint32_t i = 0; i < PressureHandler::kNumAuxHandlers; i++) {
    rt::access_once(aux_handler_states_[i].done) = true;
  }

  // Wait for aux pressure handlers to exit.
  for (uint32_t i = 0; i < PressureHandler::kNumAuxHandlers; i++) {
    while (rt::access_once(aux_handler_states_[i].done)) {
      unblock_and_relax();
    }
  }
}

void PressureHandler::wait_aux_tasks() {
  for (uint32_t i = 0; i < kNumAuxHandlers; i++) {
    while (rt::access_once(aux_handler_states_[i].task_pending)) {
      unblock_and_relax();
    }
  }
}

void PressureHandler::update_aux_handler_state(uint32_t handler_id,
                                               MigratorConn &&conn) {
  auto &state = aux_handler_states_[handler_id];
  while (rt::access_once(state.task_pending)) {
    unblock_and_relax();
  }
  barrier();
  state.conn = std::move(conn);
}

void PressureHandler::dispatch_aux_tcp_task(
    uint32_t handler_id, std::vector<iovec> &&tcp_write_task) {
  auto &state = aux_handler_states_[handler_id];
  while (rt::access_once(state.task_pending)) {
    unblock_and_relax();
  }
  state.tcp_write_task = std::move(tcp_write_task);
  store_release(&state.task_pending, true);
}

void PressureHandler::dispatch_aux_pause_task(uint32_t handler_id) {
  auto &state = aux_handler_states_[handler_id];
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
  // Prepare for the next start.
  store_release(&state->done, false);
}

std::pair<std::vector<ProcletMigrationTask>, Resource>
PressureHandler::pick_tasks(uint32_t min_num_proclets, uint32_t min_mem_mbs) {
  uint32_t picked_num = 0;
  bool done = false;
  Resource resource{.cores = 0, .mem_mbs = 0};
  auto comp = [](const ProcletMigrationTask &x, const ProcletMigrationTask &y) {
    return x.header < y.header;
  };
  std::set<ProcletMigrationTask, decltype(comp)> picked_tasks;

  auto pick_fn = [&](ProcletHeader *header, uint64_t mem_size, float cpu_load) {
    NonBlockingMigrationDisabledGuard guard(header);
    if (unlikely(!guard || !header->migratable)) {
      return;
    }
    auto [_, succeed] =
        picked_tasks.emplace(header, header->capacity, header->size());
    if (unlikely(!succeed)) {
      return;
    }
    auto size_in_mbs = mem_size / static_cast<float>(kOneMB);
    picked_num++;
    resource.mem_mbs += size_in_mbs;
    resource.cores += cpu_load;
    done =
        ((resource.mem_mbs >= min_mem_mbs) && (picked_num >= min_num_proclets));
  };

  auto traverse_fn = [&]<typename T>(T &&sorted_proclets) {
    if (sorted_proclets) {
      auto iter = sorted_proclets->begin();
      while (iter != sorted_proclets->end() && !done) {
        pick_fn(iter->header, iter->mem_size, iter->cpu_load);
        iter = sorted_proclets->erase(iter);
      }
    }
  };
  bool cpu_pressure = min_num_proclets;
  if (cpu_pressure) {
    traverse_fn(std::atomic_load(&cpu_pressure_sorted_proclets_));
  } else {
    traverse_fn(std::atomic_load(&mem_pressure_sorted_proclets_));
  }

  if (unlikely(!done)) {
    CPULoad::flush_all();
    auto all_proclets = Runtime::proclet_manager->get_all_proclets();
    auto iter = all_proclets.begin();
    while (iter != all_proclets.end() && !done) {
      auto *header = reinterpret_cast<ProcletHeader *>(*(iter++));
      auto mem_size = header->size();
      auto cpu_load = header->cpu_load.get_load();
      pick_fn(header, mem_size, cpu_load);
    }
  }

  std::vector<ProcletMigrationTask> tasks_dedupped(picked_tasks.begin(),
                                                   picked_tasks.end());
  return std::make_pair(tasks_dedupped, resource);
}

void PressureHandler::mock_set_pressure() {
  mock_ = true;
  resource_pressure_info->to_release_mem_mbs =
      std::numeric_limits<uint32_t>::max();
  store_release(&resource_pressure_info->mock, true);
}

void PressureHandler::mock_clear_pressure() {
  mock_ = false;
  resource_pressure_info->to_release_mem_mbs = 0;
  store_release(&resource_pressure_info->mock, false);
}

}  // namespace nu
