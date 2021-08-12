#pragma once

#include "../IBackend.h"
#include <physical/TransformStage.h>

namespace tuplex {

class NuBackend : public IBackend {
public:
  NuBackend(const ContextOptions &options);
  ~NuBackend();

  Executor *driver();
  void execute(PhysicalStage *stage);

public:
  inline MessageHandler &logger() const {
    return Logger::instance().logger("nu");
  }
};
} // namespace tuplex
