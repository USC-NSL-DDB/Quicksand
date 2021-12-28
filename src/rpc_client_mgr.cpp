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

RPCClientMgr::RPCClientMgr(uint16_t port) : port_(port), next_node_id_(0) {}

RPCClient *RPCClientMgr::get_client(NodeInfo info) {
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
  return get_client(info);
}

inline uint16_t get_heap_num(RemObjID rem_obj_id) {
  return (rem_obj_id - kMinHeapVAddr) / kHeapSize;
}

RPCClientMgr::NodeInfo RPCClientMgr::get_info(RemObjID rem_obj_id) {
retry:
  auto heap_num = get_heap_num(rem_obj_id);
  auto info = rem_id_to_node_info_[heap_num];

  if (unlikely(!info.raw)) {
    rt::MutexGuard g(&node_info_mutexes_[heap_num]);
    auto &info_ref = rem_id_to_node_info_[heap_num];
    if (!info_ref.raw) {
      auto optional_addr = Runtime::controller_client->resolve_obj(rem_obj_id);
      BUG_ON(!optional_addr);
      NodeInfo info;
      info.ip = optional_addr->ip;
      info.id = get_node_id_by_node_ip(info.ip);
      info_ref = info;
    }
    goto retry;
  }

  return info;
}

RPCClient *RPCClientMgr::get_by_rem_obj_id(RemObjID rem_obj_id) {
  return get_client(get_info(rem_obj_id));
}

uint32_t RPCClientMgr::get_ip_by_rem_obj_id(RemObjID rem_obj_id) {
  return get_info(rem_obj_id).ip;
}

void RPCClientMgr::update_cache(RemObjID rem_obj_id, RPCClient *old_client) {
  auto heap_num = get_heap_num(rem_obj_id);
  auto &info_ref = rem_id_to_node_info_[heap_num];
  if (info_ref.raw) {
    if (info_ref.ip != old_client->GetAddr().ip) {
      return;
    } else {
      info_ref.raw = 0;
    }
  }
}

} // namespace nu
