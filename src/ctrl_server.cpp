extern "C" {
#include <base/assert.h>
#include <net/ip.h>
}
#include <thread.h>

#include "nu/commons.hpp"
#include "nu/ctrl_server.hpp"

namespace nu {

ControllerServer::ControllerServer() {}

void ControllerServer::run_loop() {
  RPCServerInit(kControllerServerPort,
                [&](std::span<std::byte> args, RPCReturner returner) {
                  return handle_req(args, &returner);
                });
}

void ControllerServer::handle_req(std::span<std::byte> args,
                                  RPCReturner *returner) {
  auto &rpc_type = from_span<ControllerRPC_t>(args);

  switch (rpc_type) {
  case kRegisterNode: {
    auto &req = from_span<RPCReqRegisterNode>(args);
    auto resp = handle_register_node(req);
    auto span = to_span(*resp);
    returner->Return(kOk, span, [resp = std::move(resp)] {});
    break;
  }
  case kAllocateObj: {
    auto &req = from_span<RPCReqAllocateObj>(args);
    auto resp = handle_allocate_obj(req);
    auto span = to_span(*resp);
    returner->Return(kOk, span, [resp = std::move(resp)] {});
    break;
  }
  case kDestroyObj: {
    auto &req = from_span<RPCReqDestroyObj>(args);
    auto resp = handle_destroy_obj(req);
    auto span = to_span(*resp);
    returner->Return(kOk, span, [resp = std::move(resp)] {});
    break;
  }
  case kResolveObj: {
    auto &req = from_span<RPCReqResolveObj>(args);
    auto resp = handle_resolve_obj(req);
    auto span = to_span(*resp);
    returner->Return(kOk, span, [resp = std::move(resp)] {});
    break;
  }
  case kGetMigrationDest: {
    auto &req = from_span<RPCReqGetMigrationDest>(args);
    auto resp = handle_get_migration_dest(req);
    auto span = to_span(*resp);
    returner->Return(kOk, span, [resp = std::move(resp)] {});
    break;
  }
  case kUpdateLocation: {
    auto &req = from_span<RPCReqUpdateLocation>(args);
    handle_update_location(req);
    returner->Return(kOk);
    break;
  }
  default:
    BUG();
  }
}

std::unique_ptr<RPCRespRegisterNode>
ControllerServer::handle_register_node(const RPCReqRegisterNode &req) {
  auto resp = std::make_unique_for_overwrite<RPCRespRegisterNode>();
  auto node = req.node;
  resp->stack_cluster = ctrl_.register_node(node);
  return resp;
}

std::unique_ptr<RPCRespAllocateObj>
ControllerServer::handle_allocate_obj(const RPCReqAllocateObj &req) {
  auto resp = std::make_unique_for_overwrite<RPCRespAllocateObj>();
  auto optional = ctrl_.allocate_obj(req.hint);
  if (optional) {
    resp->empty = false;
    resp->id = optional->first;
    resp->server_addr = optional->second;
  } else {
    resp->empty = true;
  }
  return resp;
}

std::unique_ptr<RPCRespDestroyObj>
ControllerServer::handle_destroy_obj(const RPCReqDestroyObj &req) {
  auto resp = std::make_unique_for_overwrite<RPCRespDestroyObj>();
  ctrl_.destroy_obj(req.id);
  return resp;
}

std::unique_ptr<RPCRespResolveObj>
ControllerServer::handle_resolve_obj(const RPCReqResolveObj &req) {
  auto resp = std::make_unique_for_overwrite<RPCRespResolveObj>();
  auto addr = ctrl_.resolve_obj(req.id);
  if (addr) {
    resp->empty = false;
    resp->addr = *addr;
  } else {
    resp->empty = true;
  }
  return resp;
}

void ControllerServer::handle_update_location(const RPCReqUpdateLocation &req) {
  ctrl_.update_location(req.id, req.obj_srv_addr);
}

std::unique_ptr<RPCRespGetMigrationDest>
ControllerServer::handle_get_migration_dest(const RPCReqGetMigrationDest &req) {
  auto resp = std::make_unique_for_overwrite<RPCRespGetMigrationDest>();
  auto addr = ctrl_.get_migration_dest(req.src_ip, req.resource);
  if (addr) {
    resp->empty = false;
    resp->addr = *addr;
  } else {
    resp->empty = true;
  }
  return resp;
}

} // namespace nu
