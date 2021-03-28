#include <cstdint>
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
#include "thread.h"

#include "defs.hpp"
#include "heap_mgr.hpp"
#include "obj_server.hpp"
#include "runtime.hpp"
#include "runtime_alloc.hpp"
#include "utils/tcp.hpp"

namespace nu {

ObjServer::ObjServer() {}

ObjServer::~ObjServer() {
  if (tcp_queue_) {
    tcp_qshutdown(tcp_queue_);
    tcp_qclose(tcp_queue_);
  }
}

ObjServer::ObjServer(uint16_t port) { init(port); }

void ObjServer::init(uint16_t port) {
  port_ = port;
  netaddr addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = port};
  BUG_ON(tcp_listen(addr, kTCPListenBackLog, &tcp_queue_) != 0);
}

netaddr ObjServer::get_addr() const {
  netaddr addr = {.ip = get_cfg_ip(), .port = port_};
  return addr;
}

void ObjServer::run_loop() {
  tcpconn_t *c;
  while (tcp_accept(tcp_queue_, &c) == 0) {
    rt::Thread([&, c]() { handle_reqs(c); }).Detach();
  }
}

void ObjServer::handle_reqs(tcpconn_t *c) {
  while (true) {
    uint64_t len;
    if (!tcp_read_until(c, &len, sizeof(len))) {
      break;
    }
    std::string args_str(len, '\0');
    if (!tcp_read_until(c, args_str.data(), len)) {
      break;
    }

    std::stringstream args_ss(std::move(args_str));
    cereal::BinaryInputArchive ia(args_ss);
    GenericHandler handler;
    ia >> handler;

    handler(ia, c);
  }
  BUG_ON(tcp_shutdown(c, SHUT_RDWR) < 0);
  tcp_close(c);
}

void ObjServer::send_rpc_resp(std::stringstream &ss, tcpconn_t *rpc_conn) {
  ObjRPCRespHdr hdr;
  auto str = ss.str();
  hdr.payload_size = str.size();

  if (unlikely(thread_is_migrated())) {
    hdr.rc = FORWARDED;
    Runtime::migrator->forward_to_original_server(hdr, str.data(), rpc_conn);
  } else {
    hdr.rc = OK;
    tcp_write2_until(rpc_conn, &hdr, sizeof(hdr), str.data(), hdr.payload_size);
  }
}

} // namespace nu
