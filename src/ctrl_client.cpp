extern "C" {
#include <net/ip.h>
#include <runtime/net.h>
}

#include "nu/ctrl_client.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/migrator.hpp"
#include "nu/proclet_server.hpp"
#include "nu/runtime.hpp"

namespace nu {

ControllerClient::ControllerClient(NodeIP ctrl_server_ip, Runtime::Mode mode,
                                   lpid_t lpid)
    : lpid_(lpid),
      rpc_client_(get_runtime()->rpc_client_mgr()->get_by_ip(ctrl_server_ip)) {
  netaddr laddr{.ip = 0, .port = 0};
  netaddr raddr{.ip = ctrl_server_ip, .port = ControllerServer::kPort};
  tcp_conn_.reset(rt::TcpConn::Dial(laddr, raddr));
  BUG_ON(!tcp_conn_);

  auto md5 = get_self_md5();

  if (mode == Runtime::kServer) {
    auto optional = register_node(get_cfg_ip(), md5);
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

std::optional<std::pair<lpid_t, VAddrRange>> ControllerClient::register_node(
    NodeIP ip, MD5Val md5) {
  RPCReqRegisterNode req;
  req.ip = ip;
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

std::optional<std::pair<ProcletID, NodeIP>> ControllerClient::allocate_proclet(
    uint64_t capacity, NodeIP ip_hint) {
  RPCReqAllocateProclet req;
  req.capacity = capacity;
  req.lpid = lpid_;
  req.ip_hint = ip_hint;
  RPCReturnBuffer return_buf;
  BUG_ON(rpc_client_->Call(to_span(req), &return_buf) != kOk);
  auto &resp = from_span<RPCRespAllocateProclet>(return_buf.get_buf());
  if (resp.empty) {
    return std::nullopt;
  } else {
    auto id = resp.id;
    auto server_ip = resp.server_ip;
    return std::make_pair(id, server_ip);
  }
}

void ControllerClient::destroy_proclet(VAddrRange heap_segment) {
  RPCReqDestroyProclet req;
  req.heap_segment = heap_segment;
  RPCReturnBuffer return_buf;
  BUG_ON(rpc_client_->Call(to_span(req), &return_buf) != kOk);
}

NodeIP ControllerClient::resolve_proclet(ProcletID id) {
  RPCReqResolveProclet req;
  req.id = id;
  RPCReturnBuffer return_buf;
  BUG_ON(rpc_client_->Call(to_span(req), &return_buf) != kOk);
  auto &resp = from_span<RPCRespResolveProclet>(return_buf.get_buf());
  return resp.ip;
}

MigrationDest ControllerClient::acquire_migration_dest(Resource resource) {
  rt::SpinGuard g(&spin_);

  RPCReqAcquireMigrationDest req;
  req.lpid = lpid_;
  req.src_ip = get_cfg_ip();
  req.resource = resource;
  BUG_ON(tcp_conn_->WriteFull(&req, sizeof(req), /* nt = */ false,
                              /* poll = */ true) != sizeof(req));

  RPCRespAcquireMigrationDest resp;
  BUG_ON(tcp_conn_->ReadFull(&resp, sizeof(resp), /* nt = */ false,
                             /* poll = */ true) != sizeof(resp));
  return MigrationDest(this, resp.ip);
}

void ControllerClient::update_location(ProcletID id, NodeIP proclet_srv_ip) {
  rt::SpinGuard g(&spin_);

  RPCReqUpdateLocation req;
  req.id = id;
  req.proclet_srv_ip = proclet_srv_ip;
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

void ControllerClient::release_migration_dest(NodeIP ip) {
  rt::SpinGuard g(&spin_);

  RPCReqReleaseMigrationDest req;
  req.lpid = lpid_;
  req.ip = ip;
  BUG_ON(tcp_conn_->WriteFull(&req, sizeof(req), /* nt = */ false,
                              /* poll = */ true) != sizeof(req));
}

MigrationDest::MigrationDest(ControllerClient *client, NodeIP ip)
    : client_(client), ip_(ip) {}

MigrationDest::~MigrationDest() { client_->release_migration_dest(ip_); }

MigrationDest::operator bool() const { return ip_; }

NodeIP MigrationDest::get_ip() const { return ip_; }

}  // namespace nu
