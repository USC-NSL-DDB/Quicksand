#include <memory>
#include <vector>

#include "RuntimeInterface.h"
#include "ee/nu/NuBackend.h"
#include "physical/TransformStage.h"

namespace tuplex {

struct ExecutionBuffer {
  constexpr static size_t kResultBufferSize =
      200 * 1024 * 1024; // same as AWSLambdaBackend.

  size_t num_output_rows;
  size_t num_exception_rows;
  size_t bytes_written;
  size_t capacity;
  std::unique_ptr<uint8_t[]> buffer;

  ExecutionBuffer()
      : num_output_rows(0), num_exception_rows(0), bytes_written(0),
        capacity(kResultBufferSize) {
    buffer = std::make_unique<uint8_t[]>(capacity);
  }
};

int64_t write_row_callback(ExecutionBuffer *exec, const uint8_t *buf,
                           int64_t buf_size) {
  if (exec->bytes_written + buf_size < exec->capacity) {
    memcpy(exec->buffer.get() + exec->bytes_written, buf, buf_size);
    exec->bytes_written += buf_size;
    exec->num_output_rows++;
  }
  return 0;
}

void write_hash_callback(ExecutionBuffer *user, const uint8_t *key,
                         int64_t key_size, const uint8_t *bucket,
                         int64_t bucket_size) {
  throw std::runtime_error("writeHashCallback not supported yet in Lambda!");
}

void except_row_callback(ExecutionBuffer *exec, int64_t exception_code,
                         int64_t exception_operator_id, int64_t row_number,
                         uint8_t *input, int64_t data_length) {
  exec->num_exception_rows++;
}

NuBackend::NuBackend(const ContextOptions &options) : options_(options) {
  logger().info("Initializing NuBackend.");
  driver_.reset(new Executor(options.DRIVER_MEMORY(), options.PARTITION_SIZE(),
                             options.RUNTIME_MEMORY(),
                             options.RUNTIME_MEMORY_DEFAULT_BLOCK_SIZE(),
                             options.SCRATCH_DIR(), "nu-backend"));
  auto runtime_path = options.RUNTIME_LIBRARY().toPath();
  if (!runtime::init(runtime_path)) {
    logger().error("FATAL ERROR: Could not load runtime library");
    exit(1);
  }
}

NuBackend::~NuBackend() {}

TransformStage *NuBackend::parse_and_check_stage(PhysicalStage *stage) {
  auto tstage = dynamic_cast<TransformStage *>(stage);
  if (!tstage) {
    throw std::runtime_error("Unsupported stage.");
  }
  if (tstage->inputMode() != EndPointMode::MEMORY) {
    throw std::runtime_error("Unsupported input mode.");
  }
  if (tstage->outputMode() != EndPointMode::MEMORY) {
    throw std::runtime_error("Unsupported input mode.");
  }
  return tstage;
}

void NuBackend::register_callbacks(TransformStage *tstage,
                                   tuplex::JITCompiler *jit_compiler) {
  if (!tstage->writeMemoryCallbackName().empty())
    jit_compiler->registerSymbol(tstage->writeMemoryCallbackName(),
                                 write_row_callback);
  if (!tstage->exceptionCallbackName().empty())
    jit_compiler->registerSymbol(tstage->exceptionCallbackName(),
                                 except_row_callback);
  if (!tstage->writeFileCallbackName().empty())
    jit_compiler->registerSymbol(tstage->writeFileCallbackName(),
                                 write_row_callback);
  if (!tstage->writeHashCallbackName().empty())
    jit_compiler->registerSymbol(tstage->writeHashCallbackName(),
                                 write_hash_callback);
}

std::shared_ptr<TransformStage::JITSymbols>
NuBackend::compile_bitcode(TransformStage *tstage,
                           tuplex::JITCompiler *jit_compiler) {
  llvm::LLVMContext ctx;
  auto mod = codegen::bitCodeToModule(ctx, tstage->bitCode());
  if (!mod) {
    throw std::runtime_error("Failed to convert bitcode into module.");
  }

  std::string module_errors;
  llvm::raw_string_ostream os(module_errors);
  if (verifyModule(*mod, &os)) {
    os.flush();
    throw std::runtime_error(std::string("Failed to verify module, errors = ") +
                             module_errors + ".");
  }

  return tstage->compile(*jit_compiler, nullptr, true, false);
}

void NuBackend::init_stage(TransformStage *tstage,
                           TransformStage::JITSymbols *syms) {
  if (syms->initStageFunctor &&
      syms->initStageFunctor(
          tstage->initData().numArgs,
          reinterpret_cast<void **>(tstage->initData().hash_maps),
          reinterpret_cast<void **>(tstage->initData().null_buckets)) != 0) {
    throw std::runtime_error("initStage() failed for stage " +
                             std::to_string(tstage->number()));
  }
}

Partition *NuBackend::execute_code(TransformStage *tstage,
                                   const uint8_t *input_buf,
                                   uint64_t input_buf_size,
                                   ExecutionBuffer *execution_buf,
                                   TransformStage::JITSymbols *syms) {
  int64_t normal_row_output_count = 0;
  int64_t bad_row_output_count = 0;
  auto response_code =
      syms->functor(execution_buf, input_buf, input_buf_size,
                    &normal_row_output_count, &bad_row_output_count, false);
  Partition *partition = driver_->allocWritablePartition(
      execution_buf->bytes_written + sizeof(int64_t), tstage->outputSchema(),
      tstage->outputDataSetID());
  auto *ptr = partition->lockWrite();
  memcpy(ptr, execution_buf->buffer.get(), execution_buf->bytes_written);
  partition->unlockWrite();
  partition->setNumRows(execution_buf->num_output_rows);
  partition->setBytesWritten(execution_buf->bytes_written);
  return partition;
}

Partition *NuBackend::do_transform(TransformStage *tstage,
                                   const uint8_t *input_buf,
                                   uint64_t input_buf_size) {
  ExecutionBuffer execution_buf;
  tuplex::JITCompiler jit_compiler;
  register_callbacks(tstage, &jit_compiler);
  auto syms = compile_bitcode(tstage, &jit_compiler);
  init_stage(tstage, syms.get());
  return execute_code(tstage, input_buf, input_buf_size, &execution_buf,
                      syms.get());
}

void NuBackend::execute(PhysicalStage *stage) {
  auto tstage = parse_and_check_stage(stage);

  std::vector<Partition *> output_partitions;
  auto input_partitions = tstage->inputPartitions();
  for (uint32_t pid = 0; pid < input_partitions.size(); pid++) {
    auto *input_partition = tstage->inputPartitions()[pid];
    auto *buf = input_partition->lockRaw();
    output_partitions.push_back(
        do_transform(tstage, buf, input_partition->bytesWritten()));
    input_partition->unlock();
    input_partition->invalidate();
  }
  tstage->setMemoryResult(output_partitions);
}

} // namespace tuplex
