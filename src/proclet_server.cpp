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
#include "nu/ctrl_client.hpp"
#include "nu/migrator.hpp"
#include "nu/proclet_mgr.hpp"
#include "nu/proclet_server.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"

constexpr static bool kEnableLogging = false;
constexpr static uint32_t kPrintLoggingIntervalUs = 200 * 1000;

namespace nu {

ProcletServer::ProcletServer() {
  if constexpr (kEnableLogging) {
    trace_logger_.enable_print(kPrintLoggingIntervalUs);
  }
}

ProcletServer::~ProcletServer() {}

void ProcletServer::parse_and_run_handler(std::span<std::byte> args,
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

void ProcletServer::send_rpc_resp_ok(
    ArchivePool<RuntimeAllocator<uint8_t>>::OASStream *oa_sstream,
    RPCReturner *returner) {
  auto view = oa_sstream->ss.view();
  auto data = reinterpret_cast<const std::byte *>(view.data());
  auto len = oa_sstream->ss.tellp();

  if (likely(thread_is_at_creator())) {
    auto span = std::span(data, len);

    RuntimeSlabGuard guard;
    returner->Return(kOk, span, [oa_sstream]() {
      Runtime::archive_pool->put_oa_sstream(oa_sstream);
    });
  } else {
    Runtime::migrator->forward_to_original_server(kOk, returner, len, data);
    Runtime::archive_pool->put_oa_sstream(oa_sstream);
  }
}

void ProcletServer::send_rpc_resp_wrong_client(RPCReturner *returner) {
  if (likely(thread_is_at_creator())) {
    RuntimeSlabGuard guard;
    returner->Return(kErrWrongClient);
  } else {
    Runtime::migrator->forward_to_original_server(kErrWrongClient, returner, 0,
                                                  nullptr);
  }
}

void ProcletServer::release_proclet(VAddrRange vaddr_range) {
  rt::Spawn([vaddr_range] {
    Runtime::controller_client->destroy_proclet(vaddr_range);
  });
}

}  // namespace nu
