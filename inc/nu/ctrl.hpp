#pragma once

#include <cstdint>
#include <list>
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

// This is really a logical node as opposed to the real physical node.
struct Node {
  // TODO: add other informations, e.g., free mem size.
  uint32_t ip;
  uint16_t rpc_srv_port;
  uint16_t migrator_port;
  lpid_t lpid;

  bool operator<(const Node &o) const { return ip < o.ip; }
};

class Controller {
public:
  Controller();
  ~Controller();
  std::optional<std::pair<lpid_t, VAddrRange>> register_node(Node &node);
  std::optional<std::pair<RemObjID, netaddr>> allocate_obj(netaddr hint);
  void destroy_obj(RemObjID id);
  std::optional<netaddr> resolve_obj(RemObjID id);
  std::optional<netaddr> get_migration_dest(uint32_t requestor_ip,
                                            Resource resource);
  void update_location(RemObjID id, netaddr obj_srv_addr);

private:
  std::stack<VAddrRange> free_heap_segments_;            // One segment per RemObj.
  std::stack<VAddrRange> free_stack_cluster_segments_;   // One segment per Node.
  std::set<lpid_t> free_lpids_;
  std::unordered_map<RemObjID, netaddr> objs_map_;
  std::set<Node> nodes_;
  std::set<Node>::iterator nodes_iter_;
  rt::Mutex mutex_;

  std::optional<Node> select_node_for_obj(netaddr hint);
};
} // namespace nu
