extern "C" {
#include <net/ip.h>
#include <runtime/net.h>
}

#include "ctrl_client.hpp"
#include "ctrl_conn_mgr.hpp"
#include "runtime.hpp"

namespace nu {

ControllerClient::ControllerClient() {}

ControllerClient::ControllerClient(uint16_t local_obj_srv_port,
                                   netaddr remote_ctrl_addr, bool server_mode)
    : conn_mgr_(new ControllerConnManager(remote_ctrl_addr)) {
  if (server_mode) {
    Node node;
    node.addr = {.ip = get_cfg_ip(), .port = local_obj_srv_port};
    register_node(node);
  }
}

void ControllerClient::register_node(Node node) {
  ControllerRPC_t rpc_type = REGISTER_NODE;
  RPCReqRegisterNode req;
  RPCRespRegisterNode resp;
  req.node = node;
  auto c = conn_mgr_->get_conn();
  tcp_write2_until(c, &rpc_type, sizeof(rpc_type), &req, sizeof(req));
  tcp_read_until(c, &resp, sizeof(resp));
  conn_mgr_->put_conn(c);
}

std::optional<std::pair<RemObjID, VAddrRange>> ControllerClient::allocate_obj() {
  ControllerRPC_t rpc_type = ALLOCATE_OBJ;
  RPCReqAllocateObj req;
  RPCRespAllocateObj resp;
  auto c = conn_mgr_->get_conn();
  tcp_write2_until(c, &rpc_type, sizeof(rpc_type), &req, sizeof(req));
  tcp_read_until(c, &resp, sizeof(resp));
  conn_mgr_->put_conn(c);
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
  auto c = conn_mgr_->get_conn();  
  tcp_write2_until(c, &rpc_type, sizeof(rpc_type), &req, sizeof(req));
  tcp_read_until(c, &resp, sizeof(resp));
  conn_mgr_->put_conn(c);  
}

std::optional<netaddr> ControllerClient::resolve_obj(RemObjID id) {
  ControllerRPC_t rpc_type = RESOLVE_OBJ;
  RPCReqResolveObj req;
  RPCRespResolveObj resp;
  req.id = id;
  auto c = conn_mgr_->get_conn();    
  tcp_write2_until(c, &rpc_type, sizeof(rpc_type), &req, sizeof(req));
  tcp_read_until(c, &resp, sizeof(resp));
  conn_mgr_->put_conn(c);
  if (resp.empty) {
    return std::nullopt;
  } else {
    auto addr = resp.addr;
    return addr;
  }
}
} // namespace nu
