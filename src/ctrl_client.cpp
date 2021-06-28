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

ControllerConnManager::ControllerConnManager(uint32_t ctrl_server_ip)
    : creator_([ctrl_server_ip](bool unused) {
        netaddr local_ctrl_client_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0),
                                          .port = 0};
        netaddr remote_ctrl_server_addr = {
            .ip = ctrl_server_ip,
            .port = ControllerServer::kControllerServerPort};
        auto tcp_conn =
            rt::TcpConn::Dial(local_ctrl_client_addr, remote_ctrl_server_addr);
        BUG_ON(!tcp_conn);
        return tcp_conn;
      }),
      mgr_(creator_, kNumPerCoreCachedConns) {}

inline rt::TcpConn *ControllerConnManager::get_conn() {
  return mgr_.get_conn(false);
}

inline void ControllerConnManager::put_conn(rt::TcpConn *conn) {
  mgr_.put_conn(false, conn);
}

inline void ControllerConnManager::reserve_conns(uint32_t num) {
  mgr_.reserve_conns(false, num);
}

ControllerClient::ControllerClient(uint32_t ctrl_server_ip, Runtime::Mode mode)
    : conn_mgr_(ctrl_server_ip) {
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
  ControllerRPC_t rpc_type = REGISTER_NODE;
  RPCReqRegisterNode req;
  RPCRespRegisterNode resp;
  req.node = node;
  auto c = conn_mgr_.get_conn();
  const iovec iovecs[] = {{&rpc_type, sizeof(rpc_type)}, {&req, sizeof(req)}};
  BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
  BUG_ON(c->ReadFull(&resp, sizeof(resp)) <= 0);
  conn_mgr_.put_conn(c);
  return resp.stack_cluster;
}

std::optional<std::pair<RemObjID, netaddr>>
ControllerClient::allocate_obj(std::optional<netaddr> hint) {
  ControllerRPC_t rpc_type = ALLOCATE_OBJ;
  RPCReqAllocateObj req;
  RPCRespAllocateObj resp;
  req.hint = hint;
  auto c = conn_mgr_.get_conn();
  const iovec iovecs[] = {{&rpc_type, sizeof(rpc_type)}, {&req, sizeof(req)}};
  BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
  BUG_ON(c->ReadFull(&resp, sizeof(resp)) <= 0);
  conn_mgr_.put_conn(c);
  if (resp.empty) {
    return std::nullopt;
  } else {
    auto id = resp.id;
    auto server_addr = resp.server_addr;
    return std::make_pair(id, server_addr);
  }
}

void ControllerClient::destroy_obj(RemObjID id) {
  ControllerRPC_t rpc_type = DESTROY_OBJ;
  RPCReqDestroyObj req;
  RPCRespDestroyObj resp;
  req.id = id;
  auto c = conn_mgr_.get_conn();
  const iovec iovecs[] = {{&rpc_type, sizeof(rpc_type)}, {&req, sizeof(req)}};
  BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
  BUG_ON(c->ReadFull(&resp, sizeof(resp)) <= 0);
  conn_mgr_.put_conn(c);
}

std::optional<netaddr> ControllerClient::resolve_obj(RemObjID id) {
  ControllerRPC_t rpc_type = RESOLVE_OBJ;
  RPCReqResolveObj req;
  RPCRespResolveObj resp;
  req.id = id;
  auto c = conn_mgr_.get_conn();
  const iovec iovecs[] = {{&rpc_type, sizeof(rpc_type)}, {&req, sizeof(req)}};
  BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
  BUG_ON(c->ReadFull(&resp, sizeof(resp)) <= 0);
  conn_mgr_.put_conn(c);
  if (resp.empty) {
    return std::nullopt;
  } else {
    auto addr = resp.addr;
    return addr;
  }
}

std::optional<netaddr> ControllerClient::get_migration_dest(Resource resource) {
  ControllerRPC_t rpc_type = GET_MIGRATION_DEST;
  RPCReqGetMigrationDest req;
  RPCRespGetMigrationDest resp;
  req.resource = resource;
  auto c = conn_mgr_.get_conn();
  const iovec iovecs[] = {{&rpc_type, sizeof(rpc_type)}, {&req, sizeof(req)}};
  BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
  BUG_ON(c->ReadFull(&resp, sizeof(resp)) <= 0);
  conn_mgr_.put_conn(c);
  if (resp.empty) {
    return std::nullopt;
  } else {
    auto addr = resp.addr;
    return addr;
  }
}

void ControllerClient::update_location(RemObjID id, netaddr obj_srv_addr) {
  ControllerRPC_t rpc_type = UPDATE_LOCATION;
  RPCReqUpdateLocation req;
  RPCRespUpdateLocation resp;
  req.id = id;
  req.obj_srv_addr = obj_srv_addr;
  auto c = conn_mgr_.get_conn();
  const iovec iovecs[] = {{&rpc_type, sizeof(rpc_type)}, {&req, sizeof(req)}};
  BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
  BUG_ON(c->ReadFull(&resp, sizeof(resp)) <= 0);
  conn_mgr_.put_conn(c);
}

void ControllerClient::reserve_conns(uint32_t num) {
  conn_mgr_.reserve_conns(num);
}

VAddrRange ControllerClient::get_stack_cluster() const {
  return stack_cluster_;
}

} // namespace nu
