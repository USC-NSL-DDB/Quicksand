extern "C" {
#include <net/ip.h>
#include <runtime/net.h>
}

#include "ctrl_client.hpp"
#include "runtime.hpp"

namespace nu {

ControllerConnManager::ControllerConnManager(netaddr remote_ctrl_addr)
    : creator_([remote_ctrl_addr](bool unused) {
        tcpconn_t *tcp_conn;
        netaddr local_ctrl_client_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0),
                                          .port = 0};
        BUG_ON(tcp_dial(local_ctrl_client_addr, remote_ctrl_addr, &tcp_conn) !=
               0);
        return tcp_conn;
      }),
      mgr_(creator_) {}

inline tcpconn_t *ControllerConnManager::get_conn() {
  return mgr_.get_conn(false);
}

inline void ControllerConnManager::put_conn(tcpconn_t *conn) {
  mgr_.put_conn(false, conn);
}

ControllerClient::ControllerClient(netaddr remote_ctrl_addr)
    : conn_mgr_(remote_ctrl_addr) {}

ControllerClient::ControllerClient(uint16_t local_obj_srv_port,
                                   uint16_t local_migra_ldr_port,
                                   netaddr remote_ctrl_addr)
    : conn_mgr_(remote_ctrl_addr) {
  Node node;
  node.obj_srv_addr = {.ip = get_cfg_ip(), .port = local_obj_srv_port};
  node.migra_ldr_addr = {.ip = get_cfg_ip(), .port = local_migra_ldr_port};
  register_node(node);
}

void ControllerClient::register_node(const Node &node) {
  ControllerRPC_t rpc_type = REGISTER_NODE;
  RPCReqRegisterNode req;
  RPCRespRegisterNode resp;
  req.node = node;
  auto c = conn_mgr_.get_conn();
  tcp_write2_until(c, &rpc_type, sizeof(rpc_type), &req, sizeof(req));
  tcp_read_until(c, &resp, sizeof(resp));
  conn_mgr_.put_conn(c);
}

std::optional<std::pair<RemObjID, VAddrRange>>
ControllerClient::allocate_obj() {
  ControllerRPC_t rpc_type = ALLOCATE_OBJ;
  RPCReqAllocateObj req;
  RPCRespAllocateObj resp;
  auto c = conn_mgr_.get_conn();
  tcp_write2_until(c, &rpc_type, sizeof(rpc_type), &req, sizeof(req));
  tcp_read_until(c, &resp, sizeof(resp));
  conn_mgr_.put_conn(c);
  if (resp.empty) {
    return std::nullopt;
  } else {
    auto id = resp.id;
    auto range = resp.range;
    return std::make_pair(id, range);
  }
}

void ControllerClient::destroy_obj(RemObjID id) {
  ControllerRPC_t rpc_type = DESTROY_OBJ;
  RPCReqDestroyObj req;
  RPCRespDestroyObj resp;
  req.id = id;
  auto c = conn_mgr_.get_conn();
  tcp_write2_until(c, &rpc_type, sizeof(rpc_type), &req, sizeof(req));
  tcp_read_until(c, &resp, sizeof(resp));
  conn_mgr_.put_conn(c);
}

std::optional<netaddr> ControllerClient::resolve_obj(RemObjID id) {
  ControllerRPC_t rpc_type = RESOLVE_OBJ;
  RPCReqResolveObj req;
  RPCRespResolveObj resp;
  req.id = id;
  auto c = conn_mgr_.get_conn();
  tcp_write2_until(c, &rpc_type, sizeof(rpc_type), &req, sizeof(req));
  tcp_read_until(c, &resp, sizeof(resp));
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
  tcp_write2_until(c, &rpc_type, sizeof(rpc_type), &req, sizeof(req));
  tcp_read_until(c, &resp, sizeof(resp));
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
  tcp_write2_until(c, &rpc_type, sizeof(rpc_type), &req, sizeof(req));
  tcp_read_until(c, &resp, sizeof(resp));
  conn_mgr_.put_conn(c);
}

} // namespace nu
