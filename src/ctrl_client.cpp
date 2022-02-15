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
    : lpid_(lpid),
      rpc_client_(Runtime::rpc_client_mgr->get_by_ip(ctrl_server_ip)) {
  netaddr laddr{.ip = 0, .port = 0};
  netaddr raddr{.ip = ctrl_server_ip, .port = ControllerServer::kPort};
  tcp_conn_.reset(rt::TcpConn::Dial(laddr, raddr));
  BUG_ON(!tcp_conn_);

  auto md5 = get_self_md5();

  if (mode == Runtime::kServer) {
    Node node{get_cfg_ip()};
    auto optional = register_node(node, md5);
    BUG_ON(!optional);
    BUG_ON(lpid_ && lpid_ != optional->first);
    std::tie(lpid_, stack_cluster_) = *optional;
  } else if (mode == Runtime::kClient) {
    BUG_ON(!verify_md5(md5));
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
  req.lpid = lpid_;
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

bool ControllerClient::verify_md5(MD5Val md5) {
  RPCReqVerifyMD5 req;
  req.lpid = lpid_;
  req.md5 = md5;
  RPCReturnBuffer return_buf;
  auto rc = rpc_client_->Call(to_span(req), &return_buf);
  BUG_ON(rc != kOk);
  auto &resp = from_span<RPCRespVerifyMD5>(return_buf.get_buf());
  return resp.passed;
}

std::optional<std::pair<RemObjID, uint32_t>>
ControllerClient::allocate_obj(uint32_t ip_hint) {
  RPCReqAllocateObj req;
  req.lpid = lpid_;
  req.ip_hint = ip_hint;
  RPCReturnBuffer return_buf;
  BUG_ON(rpc_client_->Call(to_span(req), &return_buf) != kOk);
  auto &resp = from_span<RPCRespAllocateObj>(return_buf.get_buf());
  if (resp.empty) {
    return std::nullopt;
  } else {
    auto id = resp.id;
    auto server_ip = resp.server_ip;
    return std::make_pair(id, server_ip);
  }
}

void ControllerClient::destroy_obj(RemObjID id) {
  RPCReqDestroyObj req;
  req.id = id;
  RPCReturnBuffer return_buf;
  BUG_ON(rpc_client_->Call(to_span(req), &return_buf) != kOk);
}

uint32_t ControllerClient::resolve_obj(RemObjID id) {
  RPCReqResolveObj req;
  req.id = id;
  RPCReturnBuffer return_buf;
  BUG_ON(rpc_client_->Call(to_span(req), &return_buf) != kOk);
  auto &resp = from_span<RPCRespResolveObj>(return_buf.get_buf());
  return resp.ip;
}

uint32_t ControllerClient::get_migration_dest(Resource resource) {
  rt::SpinGuard g(&spin_);

  RPCReqGetMigrationDest req;
  req.lpid = lpid_;
  req.src_ip = get_cfg_ip();
  req.resource = resource;
  BUG_ON(tcp_conn_->WriteFull(&req, sizeof(req), /* nt = */ false,
                              /* poll = */ true) != sizeof(req));

  RPCRespGetMigrationDest resp;
  BUG_ON(tcp_conn_->ReadFull(&resp, sizeof(resp), /* nt = */ false,
                             /* poll = */ true) != sizeof(resp));
  return resp.ip;
}

void ControllerClient::update_location(RemObjID id, uint32_t obj_srv_ip) {
  rt::SpinGuard g(&spin_);

  RPCReqUpdateLocation req;
  req.id = id;
  req.obj_srv_ip = obj_srv_ip;
  BUG_ON(tcp_conn_->WriteFull(&req, sizeof(req), /* nt = */ false,
                              /* poll = */ true) != sizeof(req));
}

VAddrRange ControllerClient::get_stack_cluster() const {
  return stack_cluster_;
}

void ControllerClient::report_free_resource(Resource resource) {
  rt::SpinGuard g(&spin_);

  RPCReqReportFreeResource req;
  req.lpid = lpid_;
  req.ip = get_cfg_ip();
  req.resource = resource;
  BUG_ON(tcp_conn_->WriteFull(&req, sizeof(req), /* nt = */ false,
                              /* poll = */ true) != sizeof(req));
}

} // namespace nu
