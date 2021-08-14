#pragma once

#include <memory>
#include <string>

#include "../IBackend.h"
#include "ShmLayer.h"
#include "physical/TransformStage.h"

namespace tuplex {

class NuBackend : public IBackend {
public:
  NuBackend(const ContextOptions &options);
  ~NuBackend();

  Executor *driver() { return driver_.get(); }
  void execute(PhysicalStage *stage);

public:
  ContextOptions options_;
  std::unique_ptr<Executor> driver_;
  ShmLayer shm_layer_;

  inline MessageHandler &logger() const {
    return Logger::instance().logger("nu");
  }
  TransformStage *parse_and_check_stage(PhysicalStage *stage);
  Partition *do_transform(TransformStage *tstage, const uint8_t *input_buf,
                          uint64_t input_buf_len);
  std::string gen_optimized_code(const std::string &bitcode);
};
} // namespace tuplex
