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
struct NodeStatus {
  constexpr static float kEWMAWeight = 0.25;

  bool acquired;
  Resource free_resource;

  bool has_enough_resource(Resource resource) const {
    return free_resource.cores >= resource.cores &&
           free_resource.mem_mbs >= resource.mem_mbs;
  }

  void update_free_resource(Resource resource);
};

struct Node {
  NodeIP ip;
  NodeStatus status;
};

struct LPInfo {
  std::map<NodeIP, NodeStatus> node_statuses;
  std::map<NodeIP, NodeStatus>::iterator rr_iter;

  LPInfo() : rr_iter(node_statuses.end()) {}
};

struct ProcletHeapSegment {
  VAddrRange range;
  NodeIP prev_host;
};

class Controller {
 public:
  constexpr static bool kEnableBinaryVerification = true;

  Controller();
  ~Controller();
  std::optional<std::pair<lpid_t, VAddrRange>> register_node(NodeIP ip,
                                                             lpid_t lpid,
                                                             MD5Val md5);
  bool verify_md5(lpid_t lpid, MD5Val md5);
  std::optional<std::pair<ProcletID, NodeIP>> allocate_proclet(
      uint64_t capacity, lpid_t lpid, NodeIP ip_hint);
  void destroy_proclet(VAddrRange heap_segment);
  NodeIP resolve_proclet(ProcletID id);
  NodeIP acquire_migration_dest(lpid_t lpid, NodeIP requestor_ip,
                                Resource resource);
  void release_migration_dest(lpid_t lpid, NodeIP ip);
  void update_location(ProcletID id, NodeIP proclet_srv_ip);
  void report_free_resource(lpid_t lpid, NodeIP ip, Resource free_resource);

 private:
  constexpr static auto kNumProcletSegmentBuckets =
      bsr_64(kMaxProcletHeapSize) - bsr_64(kMinProcletHeapSize) + 1;
  std::stack<ProcletHeapSegment>
      free_proclet_heap_segments_[kNumProcletSegmentBuckets];
  std::stack<VAddrRange> free_stack_cluster_segments_;  // One segment per Node.
  std::set<lpid_t> free_lpids_;
  std::unordered_map<lpid_t, MD5Val> lpid_to_md5_;
  std::unordered_map<lpid_t, LPInfo> lpid_to_info_;
  std::unordered_map<ProcletID, NodeIP> proclet_id_to_ip_;
  bool done_;
  rt::Mutex mutex_;

  NodeIP select_node_for_proclet(lpid_t lpid, NodeIP ip_hint,
                                 const ProcletHeapSegment &segment);
  bool update_node(std::set<Node>::iterator iter);
};
}  // namespace nu
