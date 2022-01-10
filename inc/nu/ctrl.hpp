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
#include "nu/utils/md5.hpp"

namespace nu {

// This is really a logical node as opposed to the real physical node.
struct Node {
  // TODO: add other informations, e.g., free mem size.
  uint32_t ip;

  bool operator<(const Node &o) const { return ip < o.ip; }
};

struct LPInfo {
  std::set<Node> nodes;
  std::set<Node>::iterator rr_iter;

  LPInfo() : rr_iter(nodes.end()) {}
};

class Controller {
public:
  Controller();
  ~Controller();
  std::optional<std::pair<lpid_t, VAddrRange>>
  register_node(Node &node, lpid_t lpid, MD5Val md5);
  bool verify_md5(lpid_t lpid, MD5Val md5);
  std::optional<std::pair<RemObjID, uint32_t>> allocate_obj(lpid_t lpid,
                                                            uint32_t ip_hint);
  void destroy_obj(RemObjID id);
  uint32_t resolve_obj(RemObjID id);
  uint32_t get_migration_dest(lpid_t lpid, uint32_t requestor_ip,
                              Resource resource);
  void update_location(RemObjID id, uint32_t obj_srv_ip);

private:
  std::stack<VAddrRange> free_heap_segments_; // One segment per RemObj.
  std::stack<VAddrRange> free_stack_cluster_segments_; // One segment per Node.
  std::set<lpid_t> free_lpids_;
  std::unordered_map<lpid_t, MD5Val> lpid_to_md5_;
  std::unordered_map<lpid_t, LPInfo> lpid_to_info_;
  std::unordered_map<RemObjID, uint32_t> objs_map_;
  rt::Mutex mutex_;

  std::optional<Node> select_node_for_obj(lpid_t lpid, uint32_t ip_hint);
};
} // namespace nu
