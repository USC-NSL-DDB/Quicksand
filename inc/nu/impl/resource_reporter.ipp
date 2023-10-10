#include <iokernel/ias_constant.h>
#include <runtime.h>

#include <algorithm>

namespace nu {

inline float ResourceReporter::get_free_cores() const {
  return std::min(rt::RuntimeGlobalIdleCores(), rt::RuntimeMaxCores() -
                                                    rt::RuntimeActiveCores() +
                                                    rt::RuntimeBurningCores());
}

inline float ResourceReporter::get_free_mem_mbs() const {
  return rt::RuntimeFreeMemMbs();
}

inline float ResourceReporter::get_usable_mem_mbs() const {
  return rt::RuntimeFreeMemMbs() - 1.0 * IAS_PS_MEM_LOW_MB;
}

}  // namespace nu
