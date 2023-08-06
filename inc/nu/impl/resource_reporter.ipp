#include <runtime.h>

#include <algorithm>

namespace nu {

inline float ResourceReporter::get_free_cores() const {
  return std::min(rt::RuntimeGlobalIdleCores(),
                  rt::RuntimeMaxCores() - rt::RuntimeActiveCores());
}

inline float ResourceReporter::get_free_mem_mbs() const {
  return rt::RuntimeFreeMemMbs();
}

}  // namespace nu
