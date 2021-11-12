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
  auto optional_location =
      Runtime::controller_client->resolve_obj(rem_obj_id, 0);
  BUG_ON(!optional_location);
  ip = optional_location->addr.ip;
  gen = optional_location->gen;
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

std::pair<uint32_t, RPCClient *>
RPCClientMgr::get_by_rem_obj_id(RemObjID rem_obj_id) {
retry:
  auto *info = rem_id_to_node_info_map_.get(rem_obj_id);

  if (unlikely(!info)) {
    rem_id_to_node_info_map_.emplace_if_not_exists(rem_obj_id, rem_obj_id,
                                                   this);
    goto retry;
  }

  return std::make_pair(info->gen, get(*info));
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

void RPCClientMgr::update_cache(RemObjID rem_obj_id, uint32_t gen) {
  std::function update_fn = [&](std::pair<const RemObjID, NodeInfo> *p) {
    auto &info = p->second;
    rt::MutexGuard g(&info.mutex);

    if (unlikely(gen < info.gen)) {
      return;
    }
    BUG_ON(gen > info.gen);
    auto optional_location =
        Runtime::controller_client->resolve_obj(rem_obj_id, ++gen);
    BUG_ON(!optional_location);
    info.ip = optional_location->addr.ip;
    info.id = this->get_node_id_by_node_ip(info.ip);
    BUG_ON(gen > optional_location->gen);
    info.gen = optional_location->gen;
  };
  rem_id_to_node_info_map_.apply(rem_obj_id, update_fn);
}

} // namespace nu
