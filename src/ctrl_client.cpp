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

ControllerClient::ControllerClient(uint32_t ctrl_server_ip, Runtime::Mode mode,
                                   lpid_t lpid)
    : rpc_client_(Runtime::rpc_client_mgr->get_by_ip(ctrl_server_ip)) {
  auto md5 = get_self_md5();

  if (mode == Runtime::kServer) {
    Node node{get_cfg_ip(), RPCServer::kPort, Migrator::kPort, lpid};
    auto optional = register_node(node, md5);
    BUG_ON(!optional);
    BUG_ON(lpid && lpid != optional->first);
    std::tie(lpid_, stack_cluster_) = *optional;
  } else if (mode == Runtime::kClient) {
    BUG_ON(!verify_md5(lpid, md5));
    lpid_ = lpid;
  } else {
    BUG();
  }
  std::cout << "running with lpid = " << lpid_ << std::endl;
}

std::optional<std::pair<lpid_t, VAddrRange>>
ControllerClient::register_node(const Node &node, MD5Val md5) {
  RPCReqRegisterNode req;
  req.node = node;
  req.md5 = md5;
  RPCReturnBuffer return_buf;
  auto rc = rpc_client_->Call(to_span(req), &return_buf);
  BUG_ON(rc != kOk);
  auto &resp = from_span<RPCRespRegisterNode>(return_buf.get_buf());
  if (resp.empty) {
    return std::nullopt;
  } else {
    auto lpid = resp.lpid;
    auto stack_cluster = resp.stack_cluster;
    return std::make_pair(lpid, stack_cluster);
  }
}

bool ControllerClient::verify_md5(lpid_t lpid, MD5Val md5) {
  RPCReqVerifyMD5 req;
  req.lpid = lpid;
  req.md5 = md5;
  RPCReturnBuffer return_buf;
  auto rc = rpc_client_->Call(to_span(req), &return_buf);
  BUG_ON(rc != kOk);
  auto &resp = from_span<RPCRespVerifyMD5>(return_buf.get_buf());
  return resp.passed;
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
  BUG_ON(rpc_client_->CallPoll(to_span(req), &return_buf) != kOk);
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
  BUG_ON(rpc_client_->CallPoll(to_span(req), &return_buf) != kOk);
}

VAddrRange ControllerClient::get_stack_cluster() const {
  return stack_cluster_;
}

} // namespace nu
