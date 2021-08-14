#pragma once

#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <cstdint>
#include <memory>
#include <vector>

#include "physical/TransformStage.h"

using namespace boost::interprocess;

namespace tuplex {

class ShmLayer {
public:
  struct SReq {
    const uint8_t *input_buf;
    uint64_t input_buf_len;
    TransformStage *tstage;
  };

  struct RReq {
    uint8_t *input_buf;
    uint64_t input_buf_len;
    std::unique_ptr<TransformStage> tstage;
  };

  struct Resp {
    uint8_t *output_buf;
    uint64_t output_buf_len;
    uint64_t num_output_rows;
  };

  enum Mode { REQUESTER, RESPONSER };

  using ShmemAllocator =
      allocator<uint8_t, managed_shared_memory::segment_manager>;
  using ByteVector = std::vector<uint8_t, ShmemAllocator>;

  constexpr static auto kShmName = "NuShm";
  constexpr static uint64_t kShmSize = 1ULL << 30;
  constexpr static auto kReqQueueName = "NuReqQueue";
  constexpr static auto kRespQueueName = "NuRespQueue";
  constexpr static uint64_t kMaxMessageNum = 128;
  constexpr static uint64_t kMaxMessageSize = 1;
  constexpr static auto kInputVectorName = "NuInputVec";
  constexpr static auto kOutputVectorName = "NuOutputVec";
  constexpr static auto kTstageVectorName = "NuTstageVec";

  ShmLayer(Mode mode);
  ~ShmLayer();
  void send_req(SReq sreq);
  RReq receive_req();
  void send_resp(Resp sresp);
  Resp receive_resp();

private:
  Mode mode_;
  std::unique_ptr<managed_shared_memory> segment_;
  std::unique_ptr<ShmemAllocator> allocator_;
  std::unique_ptr<message_queue> req_queue_;
  std::unique_ptr<message_queue> resp_queue_;
};

} // namespace tuplex
