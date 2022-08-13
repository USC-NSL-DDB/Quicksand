namespace nu {

inline bool PressureHandler::has_pressure() {
  return resource_pressure_info->mem_mbs_to_release ||
         resource_pressure_info->cpu_pressure;
}

}  // namespace nu
