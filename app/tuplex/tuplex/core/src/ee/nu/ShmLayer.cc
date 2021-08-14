#include "ee/nu/ShmLayer.h"
#include "Lambda.pb.h"

namespace tuplex {

ShmLayer::ShmLayer(Mode mode) : mode_(mode) {
  if (mode == REQUESTER) {
    shared_memory_object::remove(kShmName);
    message_queue::remove(kReqQueueName);
    message_queue::remove(kRespQueueName);
    segment_.reset(new managed_shared_memory(create_only, kShmName, kShmSize));
    req_queue_.reset(new message_queue(create_only, kReqQueueName,
                                       kMaxMessageNum, kMaxMessageSize));
    resp_queue_.reset(new message_queue(create_only, kRespQueueName,
                                        kMaxMessageNum, kMaxMessageSize));
  } else {
    segment_.reset(new managed_shared_memory(open_only, kShmName));
    req_queue_.reset(new message_queue(open_only, kReqQueueName));
    resp_queue_.reset(new message_queue(open_only, kRespQueueName));
  }
  allocator_.reset(new ShmemAllocator(segment_->get_segment_manager()));
}

ShmLayer::~ShmLayer() {
  if (mode_ == REQUESTER) {
    shared_memory_object::remove(kShmName);
    message_queue::remove(kReqQueueName);
    message_queue::remove(kRespQueueName);
  }
}

void ShmLayer::send_req(SReq sreq) {
  assert(mode_ == REQUESTER);

  auto *input_vec =
      segment_->find_or_construct<ByteVector>(kInputVectorName)(*allocator_);
  assert(input_vec);
  input_vec->resize(sreq.input_buf_len);
  memcpy(input_vec->data(), sreq.input_buf, sreq.input_buf_len);

  auto *tstage_vec =
      segment_->find_or_construct<ByteVector>(kTstageVectorName)(*allocator_);
  assert(tstage_vec);
  auto tstage_pb = sreq.tstage->to_protobuf();
  auto num_tstage_bytes = tstage_pb->ByteSizeLong();
  tstage_vec->resize(num_tstage_bytes);
  tstage_pb->SerializeToArray(tstage_vec->data(), num_tstage_bytes);

  bool dummy;
  req_queue_->send(&dummy, sizeof(dummy), 0);
}

ShmLayer::RReq ShmLayer::receive_req() {
  assert(mode_ == RESPONSER);

  bool dummy;
  unsigned int priority;
  message_queue::size_type recvd_size;
  req_queue_->receive(&dummy, sizeof(dummy), recvd_size, priority);

  RReq rreq;
  auto *input_vec = segment_->find<ByteVector>(kInputVectorName).first;
  assert(input_vec);
  rreq.input_buf = input_vec->data();
  rreq.input_buf_len = input_vec->size();

  auto *tstage_vec = segment_->find<ByteVector>(kTstageVectorName).first;
  assert(tstage_vec);
  messages::TransformStage tstage_pb;
  tstage_pb.ParseFromArray(tstage_vec->data(), tstage_vec->size());
  rreq.tstage.reset(TransformStage::from_protobuf(tstage_pb));

  return rreq;
}

void ShmLayer::send_resp(Resp resp) {
  assert(mode_ == RESPONSER);

  auto *output_vec =
      segment_->find_or_construct<ByteVector>(kOutputVectorName)(*allocator_);
  assert(output_vec);
  output_vec->resize(resp.output_buf_len);
  memcpy(output_vec->data(), resp.output_buf, resp.output_buf_len);

  bool dummy;
  resp_queue_->send(&dummy, sizeof(dummy), 0);
}

ShmLayer::Resp ShmLayer::receive_resp() {
  assert(mode_ == REQUESTER);

  bool dummy;
  unsigned int priority;
  message_queue::size_type recvd_size;
  resp_queue_->receive(&dummy, sizeof(dummy), recvd_size, priority);

  Resp resp;
  auto *output_vec = segment_->find<ByteVector>(kOutputVectorName).first;
  assert(output_vec);
  resp.output_buf = output_vec->data();
  resp.output_buf_len = output_vec->size();

  return resp;
}

} // namespace Tuplex
