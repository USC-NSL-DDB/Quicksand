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
  main_handler_th_ = rt::Thread([&] {
    while (!rt::access_once(done_)) {
      timer_sleep_hp(kHandlerSleepUs);
      rt::Preempt p;
      rt::PreemptGuard g(&p);
      main_handler();
    }
  });

  for (uint32_t i = 0; i < kNumAuxHandlers; i++) {
    aux_handler_ths_[i] = rt::Thread([&, state = &aux_handler_states_[i]] {
      while (!rt::access_once(done_)) {
        timer_sleep_hp(kHandlerSleepUs);
        rt::Preempt p;
        rt::PreemptGuard g(&p);
        aux_handler(state);
      }
    });
  }

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
  main_handler_th_.Join();
  for (auto &th : aux_handler_ths_) {
    th.Join();
  }
}

Utility::Utility() {}

Utility::Utility(ProcletHeader *proclet_header) {
  header = proclet_header;

  auto proclet_size = proclet_header->size();
  auto stack_size = proclet_header->thread_cnt.get() * kStackSize;
  mem_size = proclet_size + stack_size;
  auto time = kFixedCostUs + (mem_size / (kNetBwGbps / 8.0f) / 1000.0f);

  cpu_load = proclet_header->cpu_load.get_load();
  proclet_header->cpu_load.reset();

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

void PressureHandler::main_handler() {
  bool aux_handlers_started = false;

again:
  if (!has_pressure()) {
    return;
  }

  if constexpr (kEnableLogging) {
    std::cout << "Detect pressure = { .mem_mbs = "
              << rt::RuntimeToReleaseMemMbs()
              << ", .cpu_pressure = " << rt::RuntimeCpuPressure()
              << ", .mock = " << mock_ << " }." << std::endl;
  }

  if (!aux_handlers_started) {
    aux_handlers_started = true;
    start_aux_handlers();
  }

  auto min_num_proclets =
      rt::RuntimeCpuPressure()
          ? Runtime::migrator->get_max_num_proclets_per_migration()
          : 0;
  auto min_mem_mbs = rt::RuntimeToReleaseMemMbs();
  auto [proclets, resource] = pick_proclets(min_num_proclets, min_mem_mbs);
  if (!proclets.empty()) {
    auto num_migrated = Runtime::migrator->migrate(resource, proclets);
    if constexpr (kEnableLogging) {
      std::cout << "Migrate " << num_migrated << " proclets." << std::endl;
    }
    if (num_migrated == proclets.size()) {
      goto again;
    }
  }

  mock_ = false;
  if (aux_handlers_started)   {
    stop_aux_handlers();
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

void PressureHandler::aux_handler(AuxHandlerState *state) {
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
}

std::pair<std::vector<ProcletHeader *>, Resource>
PressureHandler::pick_proclets(uint32_t min_num_proclets,
                               uint32_t min_mem_mbs) {
  uint32_t picked_num = 0;
  bool done = false;
  std::set<ProcletHeader *> picked_proclets;
  Resource resource{.cores = 0, .mem_mbs = 0};

  auto pick_fn = [&](ProcletHeader *header, uint64_t mem_size, float cpu_load) {
    NonBlockingMigrationDisabledGuard guard(header);
    if (unlikely(!guard || !header->migratable)) {
      return;
    }
    auto [_, succeed] = picked_proclets.emplace(header);
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

  std::vector<ProcletHeader *> proclets_dedupped(picked_proclets.begin(),
                                                 picked_proclets.end());
  return std::make_pair(proclets_dedupped, resource);
}

void PressureHandler::mock_set_pressure() { mock_ = true; }

}  // namespace nu
