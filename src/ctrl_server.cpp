extern "C" {
#include <base/assert.h>
#include <net/ip.h>
}
#include "thread.h"

#include "ctrl_server.hpp"

namespace nu {

ControllerServer::ControllerServer(uint16_t port) {
  netaddr addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = port};
  BUG_ON(tcp_listen(addr, kTCPListenBackLog, &tcp_queue_) != 0);
}

void ControllerServer::run_loop() {
  tcpconn_t *c;
  while (tcp_accept(tcp_queue_, &c) == 0) {
    rt::Thread([&, c]() { handle_reqs(c); }).Detach();
  }
}

bool ControllerServer::handle_one_req(ControllerRPC_t rpc_type, tcpconn_t *c) {
  switch (rpc_type) {
  case REGISTER_NODE:
    return handle_register_node(c);
  case ALLOCATE_OBJ:
    return handle_allocate_obj(c);
  case DESTROY_OBJ:
    return handle_destroy_obj(c);
  case RESOLVE_OBJ:
    return handle_resolve_obj(c);
  default:
    return false;
    BUG();
  }
}

void ControllerServer::handle_reqs(tcpconn_t *c) {
  while (true) {
    ControllerRPC_t rpc_type;
    if (!tcp_read_until(c, &rpc_type, sizeof(rpc_type))) {
      break;
    }
    if (!handle_one_req(rpc_type, c)) {
      break;
    }
  }
}

bool ControllerServer::handle_register_node(tcpconn_t *c) {
  RPCReqRegisterNode req;
  RPCRespRegisterNode resp;
  if (!tcp_read_until(c, &req, sizeof(req))) {
    return false;
  }
  ctrl_.register_node(req.node);
  return tcp_write_until(c, &resp, sizeof(resp));
}

bool ControllerServer::handle_allocate_obj(tcpconn_t *c) {
  RPCReqAllocateObj req;
  RPCRespAllocateObj resp;
  if (!tcp_read_until(c, &req, sizeof(req))) {
    return false;
  }
  auto optional = ctrl_.allocate_obj();
  if (optional) {
    resp.empty = false;
    resp.id = optional->first;
    resp.range = optional->second;
  } else {
    resp.empty = true;
  }
  return tcp_write_until(c, &resp, sizeof(resp));
}

bool ControllerServer::handle_destroy_obj(tcpconn_t *c) {
  RPCReqDestroyObj req;
  RPCRespDestroyObj resp;
  if (!tcp_read_until(c, &req, sizeof(req))) {
    return false;
  }
  ctrl_.destroy_obj(req.id);
  return tcp_write_until(c, &resp, sizeof(resp));
}

bool ControllerServer::handle_resolve_obj(tcpconn_t *c) {
  RPCReqResolveObj req;
  RPCRespResolveObj resp;
  if (!tcp_read_until(c, &req, sizeof(req))) {
    return false;
  }
  auto addr = ctrl_.resolve_obj(req.id);
  if (addr) {
    resp.empty = false;
    resp.addr = *addr;
  } else {
    resp.empty = false;
  }
  return tcp_write_until(c, &resp, sizeof(resp));
}
} // namespace nu
