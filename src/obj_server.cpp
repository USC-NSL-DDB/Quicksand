#include <cstdint>
#include <memory>
#include <sstream>
#include <tuple>
#include <type_traits>
#include <utility>

extern "C" {
#include <base/assert.h>
#include <net/ip.h>
#include <runtime/net.h>
#include <runtime/tcp.h>
}
#include <thread.h>

#include "nu/commons.hpp"
#include "nu/heap_mgr.hpp"
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

  netaddr addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = kObjServerPort};

  auto *tcp_queue = rt::TcpQueue::Listen(addr, kTCPListenBackLog);
  BUG_ON(!tcp_queue);
  tcp_queue_.reset(tcp_queue);
}

ObjServer::~ObjServer() {
  if (tcp_queue_) {
    tcp_queue_->Shutdown();
  }
}

void ObjServer::run_loop() {
  rt::TcpConn *c;
  while ((c = tcp_queue_->Accept())) {
    rt::Thread([&, c]() { handle_reqs(c); }).Detach();
  }
}

void ObjServer::handle_reqs(rt::TcpConn *c) {
  std::unique_ptr<rt::TcpConn> gc(c);

  while (true) {
    uint64_t len;
    if (c->ReadFull(&len, sizeof(len)) <= 0) {
      break;
    }

    // TODO: gc them when the thread gets migrated.
    auto *ia_sstream = Runtime::archive_pool->get_ia_sstream();
    auto &[args_ss, ia] = *ia_sstream;

    if (unlikely(args_ss.view().size() < len)) {
      args_ss.str(std::string(len, '\0'));
    }
    if (c->ReadFull(const_cast<char *>(args_ss.view().data()), len) <= 0) {
      break;
    }

    GenericHandler handler;
    ia >> handler;

    if constexpr (kEnableLogging) {
      trace_logger_.add_trace([&] { handler(ia, c); });
    } else {
      handler(ia, c);
    }

    Runtime::archive_pool->put_ia_sstream(ia_sstream);
  }

  BUG_ON(c->Shutdown(SHUT_RDWR) < 0);
}

} // namespace nu
