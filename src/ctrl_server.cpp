extern "C" {
#include <base/assert.h>
#include <net/ip.h>
}

#include <thread.h>

#include "nu/ctrl_server.hpp"

namespace nu {

ControllerServer::ControllerServer() {}

std::unique_ptr<RPCRespRegisterNode>
ControllerServer::handle_register_node(const RPCReqRegisterNode &req) {
  auto resp = std::make_unique_for_overwrite<RPCRespRegisterNode>();
  auto node = req.node;
  auto optional = ctrl_.register_node(node);
  if (optional) {
    resp->empty = false;
    resp->lpid = optional->first;
    resp->stack_cluster = optional->second;
  } else {
    resp->empty = true;
  }
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
