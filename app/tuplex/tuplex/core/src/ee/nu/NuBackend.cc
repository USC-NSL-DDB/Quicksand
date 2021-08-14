#include <memory>
#include <vector>

#include "Lambda.pb.h"
#include "ee/nu/NuBackend.h"
#include "ee/nu/ShmLayer.h"
#include "physical/TransformStage.h"

namespace tuplex {

NuBackend::NuBackend(const ContextOptions &options)
    : options_(options), shm_layer_(ShmLayer::Mode::REQUESTER) {
  logger().info("Initializing NuBackend.");
  driver_.reset(new Executor(options.DRIVER_MEMORY(), options.PARTITION_SIZE(),
                             options.RUNTIME_MEMORY(),
                             options.RUNTIME_MEMORY_DEFAULT_BLOCK_SIZE(),
                             options.SCRATCH_DIR(), "nu-backend"));
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

std::string NuBackend::gen_optimized_code(const std::string &bitcode) {
  llvm::LLVMContext ctx;
  LLVMOptimizer opt;
  auto mod = codegen::bitCodeToModule(ctx, bitcode);
  opt.optimizeModule(*mod);
  return codegen::moduleToBitCodeString(*mod);
}

Partition *NuBackend::do_transform(TransformStage *tstage,
                                   const uint8_t *input_buf,
                                   uint64_t input_buf_len) {
  if (options_.USE_LLVM_OPTIMIZER()) {
    tstage->setBitCode(gen_optimized_code(tstage->bitCode()));
  }

  ShmLayer::SReq sreq;
  sreq.input_buf = input_buf;
  sreq.input_buf_len = input_buf_len;
  sreq.tstage = tstage;
  shm_layer_.send_req(sreq);

  auto resp = shm_layer_.receive_resp();
  Partition *partition = driver_->allocWritablePartition(
      resp.output_buf_len + sizeof(int64_t), tstage->outputSchema(),
      tstage->outputDataSetID());

  auto *ptr = partition->lockWrite();
  memcpy(ptr, resp.output_buf, resp.output_buf_len);
  partition->unlockWrite();
  partition->setNumRows(resp.num_output_rows);
  partition->setBytesWritten(resp.output_buf_len);
  return partition;
}

void NuBackend::execute(PhysicalStage *stage) {
  auto tstage = parse_and_check_stage(stage);

  std::vector<Partition *> output_partitions;
  auto input_partitions = tstage->inputPartitions();
  for (uint32_t pid = 0; pid < input_partitions.size(); pid++) {
    auto *input_partition = tstage->inputPartitions()[pid];
    auto *buf = input_partition->lockRaw();
    auto output_partition =
        do_transform(tstage, buf, input_partition->bytesWritten());
    output_partitions.push_back(output_partition);
    input_partition->unlock();
    input_partition->invalidate();
  }
  tstage->setMemoryResult(output_partitions);
}

} // namespace tuplex
