#pragma once

namespace nu {

inline NodeStatus::NodeStatus(bool _isol)
    : isol(_isol),
      acquired(false),
      free_resource{0, 0},
      series(kMovingMinWinSize) {}

inline bool NodeStatus::has_enough_cpu_resource(Resource resource) const {
  return free_resource.cores >= resource.cores + 1;
}

inline bool NodeStatus::has_enough_mem_resource(Resource resource) const {
  return free_resource.mem_mbs >= resource.mem_mbs + kMemLowWaterMarkMBs;
}

inline bool NodeStatus::has_enough_resource(Resource resource) const {
  return has_enough_cpu_resource(resource) && has_enough_mem_resource(resource);
}

}  // namespace nu
