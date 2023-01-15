#pragma once

namespace nu {

inline bool NodeStatus::has_enough_resource(Resource resource) const {
  if (free_resource.mem_mbs < resource.mem_mbs) {
    return false;
  }

  if (free_resource.cores < kMinNumCores) {
    return false;
  }

  return true;
}

}  // namespace nu
