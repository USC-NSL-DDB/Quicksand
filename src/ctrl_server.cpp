extern "C" {
#include <base/assert.h>
#include <net/ip.h>
}
#include "thread.h"

#include "ctrl_server.hpp"

namespace nu {

ControllerServer::ControllerServer(uint16_t port) {
  netaddr addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = port};
  auto tcp_queue = rt::TcpQueue::Listen(addr, kTCPListenBackLog);
  BUG_ON(!tcp_queue);
  tcp_queue_.reset(tcp_queue);
}

void ControllerServer::run_loop() {
  rt::TcpConn *c;
  while ((c = tcp_queue_->Accept())) {
    rt::Thread([&, c]() { handle_reqs(c); }).Detach();
  }
}

bool ControllerServer::handle_one_req(ControllerRPC_t rpc_type,
                                      rt::TcpConn *c) {
  switch (rpc_type) {
  case REGISTER_NODE:
    return handle_register_node(c);
  case ALLOCATE_OBJ:
    return handle_allocate_obj(c);
  case DESTROY_OBJ:
    return handle_destroy_obj(c);
  case RESOLVE_OBJ:
    return handle_resolve_obj(c);
  case GET_MIGRATION_DEST:
    return handle_get_migration_dest(c);
  case UPDATE_LOCATION:
    return handle_update_location(c);
  default:
    BUG();
  }
}

void ControllerServer::handle_reqs(rt::TcpConn *c) {
  std::unique_ptr<rt::TcpConn> gc(c);

  while (true) {
    ControllerRPC_t rpc_type;
    if (c->ReadFull(&rpc_type, sizeof(rpc_type)) <= 0) {
      break;
    }
    if (!handle_one_req(rpc_type, c)) {
      break;
    }
  }

  BUG_ON(c->Shutdown(SHUT_RDWR) < 0);
}

bool ControllerServer::handle_register_node(rt::TcpConn *c) {
  RPCReqRegisterNode req;
  RPCRespRegisterNode resp;
  if (c->ReadFull(&req, sizeof(req)) <= 0) {
    return false;
  }
  auto node = req.node;
  ctrl_.register_node(node);
  return c->WriteFull(&resp, sizeof(resp)) > 0;
}

bool ControllerServer::handle_allocate_obj(rt::TcpConn *c) {
  RPCReqAllocateObj req;
  RPCRespAllocateObj resp;
  if (c->ReadFull(&req, sizeof(req)) <= 0) {
    return false;
  }
  auto optional = ctrl_.allocate_obj(req.hint);
  if (optional) {
    resp.empty = false;
    resp.id = optional->first;
    resp.range = optional->second;
  } else {
    resp.empty = true;
  }
  return c->WriteFull(&resp, sizeof(resp)) > 0;
}

bool ControllerServer::handle_destroy_obj(rt::TcpConn *c) {
  RPCReqDestroyObj req;
  RPCRespDestroyObj resp;
  if (c->ReadFull(&req, sizeof(req)) <= 0) {
    return false;
  }
  ctrl_.destroy_obj(req.id);
  return c->WriteFull(&resp, sizeof(resp)) > 0;
}

bool ControllerServer::handle_resolve_obj(rt::TcpConn *c) {
  RPCReqResolveObj req;
  RPCRespResolveObj resp;
  if (c->ReadFull(&req, sizeof(req)) <= 0) {
    return false;
  }
  auto addr = ctrl_.resolve_obj(req.id);
  if (addr) {
    resp.empty = false;
    resp.addr = *addr;
  } else {
    resp.empty = true;
  }
  return c->WriteFull(&resp, sizeof(resp)) > 0;
}

bool ControllerServer::handle_update_location(rt::TcpConn *c) {
  RPCReqUpdateLocation req;
  RPCRespUpdateLocation resp;
  if (c->ReadFull(&req, sizeof(req)) <= 0) {
    return false;
  }
  ctrl_.update_location(req.id, req.obj_srv_addr);
  return c->WriteFull(&resp, sizeof(resp)) > 0;
}

bool ControllerServer::handle_get_migration_dest(rt::TcpConn *c) {
  RPCReqGetMigrationDest req;
  RPCRespGetMigrationDest resp;
  if (c->ReadFull(&req, sizeof(req)) <= 0) {
    return false;
  }
  auto addr = ctrl_.get_migration_dest(c->RemoteAddr().ip, req.resource);
  if (addr) {
    resp.empty = false;
    resp.addr = *addr;
  } else {
    resp.empty = true;
  }
  return c->WriteFull(&resp, sizeof(resp)) > 0;
}

} // namespace nu
