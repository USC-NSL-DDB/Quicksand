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
  RPCClient *get_by_rem_obj_id(RemObjID rem_obj_id);
  RPCClient *get_by_ip(NodeIP ip);
  uint32_t get_ip_by_rem_obj_id(RemObjID rem_obj_id);
  void update_cache(RemObjID rem_obj_id, RPCClient *old_client);

private:
  union NodeInfo { // Supports atomic assignment.
    struct {
      NodeIP ip;
      NodeID id;
    };
    uint64_t raw = 0;

    NodeInfo &operator=(const NodeInfo &o) {
      raw = o.raw;
      return *this;
    }
  };
  static_assert(sizeof(NodeInfo) == sizeof(uint64_t));

  uint16_t port_;
  NodeInfo rem_id_to_node_info_[get_max_slab_id() + 1];
  rt::Mutex node_info_mutexes_[get_max_slab_id() + 1];
  std::unordered_map<NodeIP, NodeID> node_ip_to_node_id_map_;
  NodeID next_node_id_;
  std::unique_ptr<RPCClient>
      rpc_clients_[std::numeric_limits<NodeID>::max() + 1];
  rt::Mutex mutex_;

  NodeID get_node_id_by_node_ip(NodeIP ip);
  RPCClient *get_client(NodeInfo info);
  NodeInfo get_info(RemObjID rem_obj_id);
};
} // namespace nu
