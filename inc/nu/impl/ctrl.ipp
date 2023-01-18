#pragma once

namespace nu {

inline bool NodeStatus::has_enough_cpu_resource(Resource resource) const {
  return free_resource.cores >= resource.cores + kNumCoresLowWaterMark;
}

inline bool NodeStatus::has_enough_mem_resource(Resource resource) const {
  return free_resource.mem_mbs >= resource.mem_mbs + kMemMBsLowWaterMark;
}

inline bool NodeStatus::has_enough_resource(Resource resource) const {
  return has_enough_cpu_resource(resource) && has_enough_mem_resource(resource);
}

inline bool NodeStatus::is_not_congested() const {
  return free_resource.cores >= kNumCoresLowWaterMark &&
         free_resource.mem_mbs >= kMemMBsLowWaterMark;
}

}  // namespace nu
