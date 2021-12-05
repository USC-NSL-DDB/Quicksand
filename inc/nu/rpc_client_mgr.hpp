#pragma once

#include <limits>
#include <memory>
#include <sync.h>
#include <unordered_map>
#include <utility>

#include "nu/runtime_alloc.hpp"
#include "nu/utils/rcu_hash_map.hpp"
#include "nu/utils/rpc.hpp"

namespace nu {

using NodeIP = uint32_t;
using NodeID = uint16_t;

class RPCClientMgr {
public:
  RPCClientMgr(uint16_t port);
  std::pair<uint32_t, RPCClient *> get_by_rem_obj_id(RemObjID rem_obj_id);
  RPCClient *get_by_ip(NodeIP ip);
  uint32_t get_ip_by_rem_obj_id(RemObjID rem_obj_id);
  void update_cache(RemObjID rem_obj_id, uint32_t gen);

private:
  struct NodeInfo {
    NodeInfo() {}
    NodeInfo(RemObjID rem_obj_id, RPCClientMgr *mgr);
    NodeIP ip;
    NodeID id;
    uint32_t gen;
    rt::Mutex mutex;
  };

  uint16_t port_;
  RCUHashMap<RemObjID, NodeInfo> rem_id_to_node_info_map_;
  std::unordered_map<NodeIP, NodeID> node_ip_to_node_id_map_;
  NodeID next_node_id_;
  std::unique_ptr<RPCClient>
      rpc_clients_[std::numeric_limits<NodeID>::max() + 1];
  rt::Mutex mutex_;
  friend class NodeInfo;

  NodeID get_node_id_by_node_ip(NodeIP ip);
  RPCClient *get(const NodeInfo &info);
};
} // namespace nu
