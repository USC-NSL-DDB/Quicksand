#include "nu/rpc_client_mgr.hpp"
#include "nu/ctrl_client.hpp"

namespace nu {

NodeID RPCClientMgr::get_node_id_by_node_ip(NodeIP ip) {
  NodeID id;
  rt::ScopedLock<rt::Mutex> guard(&mutex_);
  if (node_ip_to_node_id_map_.find(ip) == node_ip_to_node_id_map_.end()) {
    id = node_ip_to_node_id_map_[ip] = next_node_id_++;
    BUG_ON(!next_node_id_); // Overflow.
  } else {
    id = node_ip_to_node_id_map_[ip];
  }
  return id;
}

RPCClientMgr::NodeInfo::NodeInfo(RemObjID rem_obj_id, RPCClientMgr *mgr) {
  auto optional_addr = Runtime::controller_client->resolve_obj(rem_obj_id);
  BUG_ON(!optional_addr);
  ip = optional_addr->ip;
  id = mgr->get_node_id_by_node_ip(ip);
}

RPCClientMgr::RPCClientMgr(uint16_t port) : port_(port), next_node_id_(0) {}

RPCClient *RPCClientMgr::get(const NodeInfo &info) {
  auto &client = rpc_clients_[info.id];
  if (unlikely(!client)) {
    rt::ScopedLock<rt::Mutex> guard(&mutex_);
    if (likely(!client)) {
      client = RPCClient::Dial(netaddr(info.ip, port_));
    }
  }

  return client.get();
}

RPCClient *RPCClientMgr::get_by_ip(NodeIP ip) {
  NodeInfo info;
  info.ip = ip;
  info.id = get_node_id_by_node_ip(ip);
  return get(info);
}

RPCClient *RPCClientMgr::get_by_rem_obj_id(RemObjID rem_obj_id) {
retry:
  auto *info = rem_id_to_node_info_map_.get(rem_obj_id);

  if (unlikely(!info)) {
    rem_id_to_node_info_map_.emplace_if_not_exists(rem_obj_id, rem_obj_id,
                                                   this);
    goto retry;
  }

  return get(*info);
}

uint32_t RPCClientMgr::get_ip_by_rem_obj_id(RemObjID rem_obj_id) {
retry:
  auto *info = rem_id_to_node_info_map_.get(rem_obj_id);

  if (unlikely(!info)) {
    rem_id_to_node_info_map_.emplace_if_not_exists(rem_obj_id, rem_obj_id,
                                                   this);
    goto retry;
  }

  return info->ip;
}

void RPCClientMgr::invalidate_cache(RemObjID rem_obj_id) {
  rem_id_to_node_info_map_.remove(rem_obj_id);
}

} // namespace nu
