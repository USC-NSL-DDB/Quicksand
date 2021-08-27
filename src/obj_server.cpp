#include <cstdint>
#include <memory>
#include <sstream>
#include <tuple>
#include <type_traits>
#include <utility>

extern "C" {
#include <base/assert.h>
}
#include <thread.h>

#include "nu/commons.hpp"
#include "nu/heap_mgr.hpp"
#include "nu/migrator.hpp"
#include "nu/obj_server.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"

constexpr static bool kEnableLogging = false;
constexpr static uint32_t kPrintLoggingIntervalUs = 200 * 1000;

namespace nu {

ObjServer::ObjServer() {
  if constexpr (kEnableLogging) {
    trace_logger_.enable_print(kPrintLoggingIntervalUs);
  }
}

ObjServer::~ObjServer() {}

void ObjServer::parse_and_run_handler(std::span<std::byte> args,
                                      RPCReturner *returner) {
  // TODO: gc them when the thread gets migrated.
  auto *ia_sstream = Runtime::archive_pool->get_ia_sstream();
  auto &[args_ss, ia] = *ia_sstream;
  args_ss.span({reinterpret_cast<char *>(args.data()), args.size()});

  GenericHandler handler;
  ia >> handler;

  if constexpr (kEnableLogging) {
    trace_logger_.add_trace([&] { handler(ia, returner); });
  } else {
    handler(ia, returner);
  }

  Runtime::archive_pool->put_ia_sstream(ia_sstream);
}

void ObjServer::forward(RPCReturnCode rc, RPCReturner *returner,
                        const void *payload, uint64_t payload_len) {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  auto req_buf_len = sizeof(RPCReqForward) + payload_len;
  auto req_buf = std::make_unique_for_overwrite<std::byte[]>(req_buf_len);
  auto *req = reinterpret_cast<RPCReqForward *>(req_buf.get());
  std::construct_at(req);
  req->rc = rc;
  req->returner = *returner;
  req->stack_top = get_obj_stack_range(thread_self()).end;
  req->payload_len = payload_len;
  memcpy(req->payload, payload, payload_len);
  RPCReturnBuffer return_buf;
  auto req_span = std::span(req_buf.get(), req_buf_len);
  {
    RuntimeHeapGuard guard;
    auto *client =
        Runtime::rpc_client_mgr->get_by_ip(heap_header->old_server_ip);
    BUG_ON(client->Call(req_span, &return_buf) != kOk);
  }
  heap_header->forward_wg.Done();
}

void ObjServer::send_rpc_resp_ok(
    ArchivePool<RuntimeAllocator<uint8_t>>::OASStream *oa_sstream,
    RPCReturner *returner) {
  auto view = oa_sstream->ss.view();
  auto data = reinterpret_cast<const std::byte *>(view.data());
  auto len = oa_sstream->ss.tellp();

  if (unlikely(thread_is_migrated())) {
    forward(kOk, returner, data, len);
    Runtime::archive_pool->put_oa_sstream(oa_sstream);
  } else {
    auto span = std::span(data, len);

    RuntimeHeapGuard guard;
    returner->Return(kOk, span, [oa_sstream]() {
      Runtime::archive_pool->put_oa_sstream(oa_sstream);
    });
  }
}

void ObjServer::send_rpc_resp_wrong_client(RPCReturner *returner) {
  if (unlikely(thread_is_migrated())) {
    forward(kErrWrongClient, returner, nullptr, 0);
  } else {
    returner->Return(kErrWrongClient);
  }
}

} // namespace nu
