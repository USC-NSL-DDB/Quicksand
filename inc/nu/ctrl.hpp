#pragma once

#include <cstdint>
#include <list>
#include <optional>
#include <set>
#include <stack>
#include <unordered_map>
#include <utility>

extern "C" {
#include <runtime/net.h>
#include <runtime/tcp.h>
}
#include <net.h>
#include <sync.h>

#include "nu/commons.hpp"
#include "nu/heap_mgr.hpp"
#include "nu/rpc_client_mgr.hpp"
#include "nu/utils/netaddr.hpp"

namespace nu {

// TODO: cleanup.
struct Node {
  // TODO: add other informations, e.g., free mem size.
  netaddr obj_srv_addr;
  netaddr migrator_addr;

  bool operator<(const Node &o) const { return obj_srv_addr < o.obj_srv_addr; }
};

struct ObjLocation {
  uint32_t gen;
  netaddr addr;
};

class Controller {
public:
  Controller();
  ~Controller();
  VAddrRange register_node(Node &node);
  std::optional<std::pair<RemObjID, netaddr>> allocate_obj(netaddr hint);
  void destroy_obj(RemObjID id);
  std::optional<ObjLocation> resolve_obj(RemObjID id, uint32_t min_gen);
  std::optional<netaddr> get_migration_dest(uint32_t requestor_ip,
                                            Resource resource);
  void update_location(RemObjID id, netaddr obj_srv_addr);

private:
  std::stack<uint64_t> free_heap_segments_;            // One range per RemObj.
  std::stack<VAddrRange> free_stack_cluster_segments_; // One range per server.
  std::unordered_map<RemObjID, std::pair<rt::CondVar, ObjLocation>> objs_map_;
  std::set<Node> nodes_;
  std::set<Node>::iterator nodes_iter_;
  rt::Mutex mutex_;

  std::optional<Node> select_node_for_obj(netaddr hint);
};
} // namespace nu
