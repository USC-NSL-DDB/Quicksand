#include "nu/rpc_client_mgr.hpp"
#include "nu/ctrl_client.hpp"

namespace nu {

RPCClientMgr::RPCClientMgr(uint16_t port) : port_(port), next_node_id_(0) {}

RPCClient *RPCClientMgr::get(NodeIP ip) {
retry:
  auto *id = node_ip_to_node_id_map_.get(ip);
  if (unlikely(!id)) {
    rt::ScopedLock<rt::Mutex> guard(&mutex_);
    if (!node_ip_to_node_id_map_.get(ip)) {
      rpc_clients_[next_node_id_] = RPCClient::Dial(netaddr(ip, port_));
      node_ip_to_node_id_map_.put(ip, next_node_id_);
      return rpc_clients_[next_node_id_++].get();
    }
    goto retry;
  }
  return rpc_clients_[*id].get();
}

RemObjRPCClientMgr::NodeInfo::NodeInfo(RemObjID rem_obj_id,
                                       RemObjRPCClientMgr *mgr) {
  auto optional_addr = Runtime::controller_client->resolve_obj(rem_obj_id);
  BUG_ON(!optional_addr);
  ip = optional_addr->ip;
  rt::ScopedLock<rt::Mutex> guard(&mgr->mutex_);
  if (mgr->node_ip_to_node_id_map_.find(ip) ==
      mgr->node_ip_to_node_id_map_.end()) {
    id = mgr->node_ip_to_node_id_map_[ip] = mgr->next_node_id_++;
    BUG_ON(!mgr->next_node_id_); // Overflow.
  } else {
    id = mgr->node_ip_to_node_id_map_[ip];
  }
}

RemObjRPCClientMgr::RemObjRPCClientMgr(uint16_t port)
    : port_(port), next_node_id_(0) {}

RPCClient *RemObjRPCClientMgr::get(RemObjID rem_obj_id) {
retry:
  auto *info = rem_id_to_node_info_map_.get(rem_obj_id);

  if (unlikely(!info)) {
    rem_id_to_node_info_map_.emplace_if_not_exists(rem_obj_id, rem_obj_id,
                                                   this);
    goto retry;
  }

  auto *client = &rpc_clients_[info->id];
  if (unlikely(!*client)) {
    rt::ScopedLock<rt::Mutex> guard(&mutex_);
    client = &rpc_clients_[info->id];
    if (!*client) {
      *client = RPCClient::Dial(netaddr(info->ip, port_));
    }
  }

  return client->get();
}

void RemObjRPCClientMgr::invalidate_cache(RemObjID rem_obj_id) {
  rem_id_to_node_info_map_.remove(rem_obj_id);
}

} // namespace nu
