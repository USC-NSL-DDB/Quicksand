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
#include "nu/proclet_mgr.hpp"
#include "nu/rpc_client_mgr.hpp"
#include "nu/utils/md5.hpp"

namespace nu {

// This is really a logical node as opposed to the real physical node.
struct Node {
  // TODO: add other informations, e.g., free mem size.
  uint32_t ip;
  Resource free_resource;

  bool operator<(const Node &o) const { return ip < o.ip; }

  bool has_enough_resource(Resource resource) const {
    return free_resource.cores >= resource.cores &&
           free_resource.mem_mbs >= resource.mem_mbs;
  }

  void update_free_resource(Resource resource);
};

struct LPInfo {
  std::set<Node> nodes;
  std::set<Node>::iterator rr_iter;

  LPInfo() : rr_iter(nodes.end()) {}
};

class Controller {
 public:
  constexpr static bool kEnableBinaryVerification = true;

  Controller();
  ~Controller();
  std::optional<std::pair<lpid_t, VAddrRange>> register_node(Node &node,
                                                             lpid_t lpid,
                                                             MD5Val md5);
  bool verify_md5(lpid_t lpid, MD5Val md5);
  std::optional<std::pair<ProcletID, uint32_t>> allocate_proclet(
      lpid_t lpid, uint32_t ip_hint);
  void destroy_proclet(ProcletID id);
  uint32_t resolve_proclet(ProcletID id);
  uint32_t get_migration_dest(lpid_t lpid, uint32_t requestor_ip,
                              Resource resource);
  void update_location(ProcletID id, uint32_t proclet_srv_ip);
  void report_free_resource(lpid_t lpid, uint32_t ip, Resource free_resource);

 private:
  std::stack<VAddrRange>
      free_proclet_heap_segments_;  // One segment per Proclet.
  std::stack<VAddrRange> free_stack_cluster_segments_;  // One segment per Node.
  std::set<lpid_t> free_lpids_;
  std::unordered_map<lpid_t, MD5Val> lpid_to_md5_;
  std::unordered_map<lpid_t, LPInfo> lpid_to_info_;
  std::unordered_map<ProcletID, uint32_t> proclet_id_to_ip_;
  bool done_;
  rt::Mutex mutex_;

  std::optional<Node> select_node_for_proclet(lpid_t lpid, uint32_t ip_hint);
  bool update_node(std::set<Node>::iterator iter);
};
}  // namespace nu
