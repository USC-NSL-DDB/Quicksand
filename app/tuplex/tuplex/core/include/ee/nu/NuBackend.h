#pragma once

#include <memory>

#include "../IBackend.h"
#include <physical/TransformStage.h>

namespace tuplex {

struct ExecutionBuffer;

class NuBackend : public IBackend {
public:
  NuBackend(const ContextOptions &options);
  ~NuBackend();

  Executor *driver() { return driver_.get(); }
  void execute(PhysicalStage *stage);

public:
  ContextOptions options_;
  std::unique_ptr<Executor> driver_;

  inline MessageHandler &logger() const {
    return Logger::instance().logger("nu");
  }
  TransformStage *parse_and_check_stage(PhysicalStage *stage);
  void register_callbacks(TransformStage *tstage,
                          tuplex::JITCompiler *jit_compiler);
  std::shared_ptr<TransformStage::JITSymbols>
  compile_bitcode(TransformStage *tstage, tuplex::JITCompiler *jit_compiler);
  void init_stage(TransformStage *tstage, TransformStage::JITSymbols *syms);
  Partition *execute_code(TransformStage *tstage, const uint8_t *input_buf,
                          uint64_t input_buf_size,
                          ExecutionBuffer *execution_buf,
                          TransformStage::JITSymbols *syms);
  Partition *do_transform(TransformStage *tstage, const uint8_t *input_buf,
                          uint64_t input_buf_size);
};
} // namespace tuplex
