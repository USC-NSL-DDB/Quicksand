extern "C" {
#include <net/ip.h>
#include <runtime/net.h>
}

#include "nu/ctrl_client.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/migrator.hpp"
#include "nu/obj_server.hpp"
#include "nu/runtime.hpp"

namespace nu {

ControllerClient::ControllerClient(uint32_t ctrl_server_ip, Runtime::Mode mode)
    : rpc_client_(Runtime::rpc_client_mgr->get_by_ip(ctrl_server_ip)) {
  switch (mode) {
  case Runtime::SERVER:
    Node node;
    node.obj_srv_addr = {.ip = get_cfg_ip(), .port = ObjServer::kObjServerPort};
    node.migrator_addr = {.ip = get_cfg_ip(),
                          .port = Migrator::kMigratorServerPort};
    stack_cluster_ = register_node(node);
    break;
  case Runtime::CLIENT:
    break;
  default:
    BUG();
  }
}

VAddrRange ControllerClient::register_node(const Node &node) {
  RPCReqRegisterNode req;
  req.node = node;
  RPCReturnBuffer return_buf;
  auto rc = rpc_client_->Call(to_span(req), &return_buf);
  BUG_ON(rc != kOk);
  auto &resp = from_span<RPCRespRegisterNode>(return_buf.get_buf());
  return resp.stack_cluster;
}

std::optional<std::pair<RemObjID, netaddr>>
ControllerClient::allocate_obj(std::optional<netaddr> hint) {
  RPCReqAllocateObj req;
  req.hint = hint ? *hint : netaddr(0, 0);
  RPCReturnBuffer return_buf;
  BUG_ON(rpc_client_->Call(to_span(req), &return_buf) != kOk);
  auto &resp = from_span<RPCRespAllocateObj>(return_buf.get_buf());
  if (resp.empty) {
    return std::nullopt;
  } else {
    auto id = resp.id;
    auto server_addr = resp.server_addr;
    return std::make_pair(id, server_addr);
  }
}

void ControllerClient::destroy_obj(RemObjID id) {
  RPCReqDestroyObj req;
  req.id = id;
  RPCReturnBuffer return_buf;
  BUG_ON(rpc_client_->Call(to_span(req), &return_buf) != kOk);
}

std::optional<netaddr> ControllerClient::resolve_obj(RemObjID id) {
  RPCReqResolveObj req;
  req.id = id;
  RPCReturnBuffer return_buf;
  BUG_ON(rpc_client_->Call(to_span(req), &return_buf) != kOk);
  auto &resp = from_span<RPCRespResolveObj>(return_buf.get_buf());
  if (resp.empty) {
    return std::nullopt;
  } else {
    auto addr = resp.addr;
    return addr;
  }
}

std::optional<netaddr> ControllerClient::get_migration_dest(Resource resource) {
  RPCReqGetMigrationDest req;
  req.src_ip = get_cfg_ip();
  req.resource = resource;
  RPCReturnBuffer return_buf;
  BUG_ON(rpc_client_->Call(to_span(req), &return_buf) != kOk);
  auto &resp = from_span<RPCRespGetMigrationDest>(return_buf.get_buf());
  if (resp.empty) {
    return std::nullopt;
  } else {
    auto addr = resp.addr;
    return addr;
  }
}

void ControllerClient::update_location(RemObjID id, netaddr obj_srv_addr) {
  RPCReqUpdateLocation req;
  req.id = id;
  req.obj_srv_addr = obj_srv_addr;
  RPCReturnBuffer return_buf;
  BUG_ON(rpc_client_->Call(to_span(req), &return_buf) != kOk);
}

VAddrRange ControllerClient::get_stack_cluster() const {
  return stack_cluster_;
}

} // namespace nu
