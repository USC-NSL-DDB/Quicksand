#include "ee/nu/NuBackend.h"

namespace tuplex {

NuBackend::NuBackend(const ContextOptions &options) {
  logger().info("Init NuBackend.");
}

NuBackend::~NuBackend() {}

Executor *NuBackend::driver() { return nullptr; }

void NuBackend::execute(PhysicalStage *stage) {}

} // namespace tuplex
