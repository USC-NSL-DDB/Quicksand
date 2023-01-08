#pragma once

namespace nu {

inline bool NodeStatus::has_enough_resource(Resource resource) const {
  if (free_resource.mem_mbs < resource.mem_mbs) {
    return false;
  }

  if (!static_cast<int>(free_resource.cores) &&
      static_cast<int>(resource.cores)) {
    return false;
  }

  return true;
}

}  // namespace nu
