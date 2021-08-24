#pragma once

#include <limits>
#include <memory>
#include <sync.h>
#include <unordered_map>
#include <utility>

#include "nu/utils/rpc.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/utils/rcu_hash_map.hpp"

namespace nu {

using NodeIP = uint32_t;
using NodeID = uint16_t;

class RPCClientMgr {
public:
  RPCClientMgr(uint16_t port);
  RPCClient *get(NodeIP ip);

private:
  uint16_t port_;
  RCUHashMap<NodeIP, NodeID, RuntimeAllocator<std::pair<const NodeIP, NodeID>>>
      node_ip_to_node_id_map_;
  NodeID next_node_id_;
  std::unique_ptr<RPCClient>
      rpc_clients_[std::numeric_limits<NodeID>::max() + 1];
  rt::Mutex mutex_;
};

class RemObjRPCClientMgr {
public:
  RemObjRPCClientMgr(uint16_t port);
  RPCClient *get(RemObjID rem_obj_id);
  void invalidate_cache(RemObjID rem_obj_id);

private:
  struct NodeInfo {
    NodeInfo(RemObjID rem_obj_id, RemObjRPCClientMgr *mgr);
    NodeIP ip;
    NodeID id;
  };

  uint16_t port_;
  RCUHashMap<RemObjID, NodeInfo,
             RuntimeAllocator<std::pair<const RemObjID, NodeInfo>>>
      rem_id_to_node_info_map_;
  std::unordered_map<NodeIP, NodeID> node_ip_to_node_id_map_;
  NodeID next_node_id_;
  std::unique_ptr<RPCClient>
      rpc_clients_[std::numeric_limits<NodeID>::max() + 1];
  rt::Mutex mutex_;
};
} // namespace nu
