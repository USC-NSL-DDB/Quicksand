#include <iostream>

#include <sync.h>

#include "nu/migrator.hpp"
#include "nu/pressure_handler.hpp"
#include "nu/runtime.hpp"

constexpr static bool kEnableLogging = false;

namespace nu {

void PressureHandler::handle() {
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

  store_release(&pressure.status, HANDLED);
}

void PressureHandler::mock_set_pressure(ResourcePressureInfo pressure) {
  RuntimeHeapGuard guard;

  *resource_pressure_info = pressure;
  rt::Thread([] { handle(); }).Detach();
}

} // namespace nu
