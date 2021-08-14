#include <iostream>
#include <runtime.h>

#include "JITCompiler.h"
#include "Lambda.pb.h"
#include "RuntimeInterface.h"
#include "ee/nu/ShmLayer.h"
#include "physical/TransformStage.h"

namespace tuplex {

constexpr static auto kRuntimePath =
    "/usr/local/lib/python3.8/dist-packages/"
    "tuplex-0.3.1-py3.8-linux-x86_64.egg/tuplex/libexec/"
    "tuplex_runtime.cpython-38-x86_64-linux-gnu.so";

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

  void reset() {
    num_output_rows = num_exception_rows = bytes_written = 0;
    capacity = kResultBufferSize;
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

void register_callbacks(TransformStage *tstage,
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
compile_bitcode(TransformStage *tstage, tuplex::JITCompiler *jit_compiler) {
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

void init_stage(TransformStage *tstage, TransformStage::JITSymbols *syms) {
  if (syms->initStageFunctor &&
      syms->initStageFunctor(
          tstage->initData().numArgs,
          reinterpret_cast<void **>(tstage->initData().hash_maps),
          reinterpret_cast<void **>(tstage->initData().null_buckets)) != 0) {
    throw std::runtime_error("initStage() failed for stage " +
                             std::to_string(tstage->number()));
  }
}

void execute_code(TransformStage *tstage, const uint8_t *input_buf,
                  uint64_t input_buf_len, ExecutionBuffer *exec_buf,
                  TransformStage::JITSymbols *syms) {
  int64_t normal_row_output_count = 0;
  int64_t bad_row_output_count = 0;
  auto response_code =
      syms->functor(exec_buf, input_buf, input_buf_len,
                    &normal_row_output_count, &bad_row_output_count, false);
}

ShmLayer::Resp process_req(ExecutionBuffer *exec_buf, ShmLayer::RReq *req) {
  tuplex::JITCompiler jit_compiler;
  register_callbacks(req->tstage.get(), &jit_compiler);
  auto syms = compile_bitcode(req->tstage.get(), &jit_compiler);

  init_stage(req->tstage.get(), syms.get());
  execute_code(req->tstage.get(), req->input_buf, req->input_buf_len, exec_buf,
               syms.get());
  ShmLayer::Resp resp;
  resp.output_buf = exec_buf->buffer.get();
  resp.output_buf_len = exec_buf->bytes_written;
  resp.num_output_rows = exec_buf->num_output_rows;
  return resp;
}

void do_work() {
  ShmLayer shm_layer(ShmLayer::Mode::RESPONSER);
  if (!runtime::init(kRuntimePath)) {
    std::cerr << "FATAL ERROR: Could not load runtime library" << std::endl;
    exit(1);
  }
  ExecutionBuffer exec_buf;

  while (true) {
    auto req = shm_layer.receive_req();
    auto resp = process_req(&exec_buf, &req);
    shm_layer.send_resp(resp);
    exec_buf.reset();
  }
}

} // namespace tuplex

int main(int argc, char **argv) {
  int ret;

  if (argc < 2) {
    std::cerr << "usage: [cfg_file]" << std::endl;
    return -EINVAL;
  }

  ret = rt::RuntimeInit(std::string(argv[1]), [] { tuplex::do_work(); });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}
