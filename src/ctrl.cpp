#include <cereal/archives/binary.hpp>
#include <cstdint>
#include <limits>

extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
#include <net/ip.h>
}
#include <thread.h>

#include "nu/commons.hpp"
#include "nu/ctrl.hpp"
#include "nu/migrator.hpp"
#include "nu/obj_server.hpp"

namespace nu {

Controller::Controller() {
  for (lpid_t lpid = 1; lpid < std::numeric_limits<lpid_t>::max(); lpid++) {
    free_lpids_.insert(lpid);
  }

  for (uint64_t start_addr = kMinHeapVAddr;
       start_addr + kHeapSize <= kMaxHeapVAddr; start_addr += kHeapSize) {
    VAddrRange range = {.start = start_addr, .end = start_addr + kHeapSize};
    free_heap_segments_.push(range);
  }

  for (uint64_t start_addr = kMinStackClusterVAddr;
       start_addr + kStackClusterSize <= kMaxStackClusterVAddr;
       start_addr += kStackClusterSize) {
    VAddrRange range = {.start = start_addr,
                        .end = start_addr + kStackClusterSize};
    free_stack_cluster_segments_.push(range);
  }
}

Controller::~Controller() {}

std::optional<std::pair<lpid_t, VAddrRange>>
Controller::register_node(Node &node, lpid_t lpid, MD5Val md5) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  // TODO: should GC the allocated lpid and stack somehow through heartbeat or an
  // explicit deregister_node() call.
  if (lpid) {
    auto iter = free_lpids_.find(lpid);
    if (iter == free_lpids_.end()) {
      if (unlikely(lpid_to_md5_[lpid] != md5)) {
        return std::nullopt;
      }
    } else {
      free_lpids_.erase(iter);
      lpid_to_md5_[lpid] = md5;
    }
  } else {
    if (unlikely(free_lpids_.empty())) {
      return std::nullopt;
    }
    auto begin_iter = free_lpids_.begin();
    lpid = *begin_iter;
    free_lpids_.erase(begin_iter);
  }

  if (unlikely(free_stack_cluster_segments_.empty())) {
    free_lpids_.insert(lpid);
    return std::nullopt;
  }

  auto stack_cluster = free_stack_cluster_segments_.top();
  free_stack_cluster_segments_.pop();

  auto &nodes = lpid_to_info_[lpid].nodes;
  for (auto old_node : nodes) {
    auto *client = Runtime::rpc_client_mgr->get_by_ip(old_node.ip);
    RPCReqReserveConns req;
    RPCReturnBuffer return_buf;
    req.dest_server_addr = netaddr{node.ip, node.migrator_port};
    BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
  }

  auto *client = Runtime::rpc_client_mgr->get_by_ip(node.ip);
  for (auto old_node : nodes) {
    RPCReqReserveConns req;
    RPCReturnBuffer return_buf;
    req.dest_server_addr = netaddr{old_node.ip, old_node.migrator_port};
    BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
  }

  nodes.insert(node);
  return std::make_pair(lpid, stack_cluster);
}

bool Controller::verify_md5(lpid_t lpid, MD5Val md5) {
  return lpid_to_md5_[lpid] == md5;
}

std::optional<std::pair<RemObjID, netaddr>>
Controller::allocate_obj(lpid_t lpid, uint32_t ip_hint) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  if (unlikely(free_heap_segments_.empty())) {
    return std::nullopt;
  }
  auto start_addr = free_heap_segments_.top().start;
  auto id = start_addr;
  free_heap_segments_.pop();
  auto node_optional = select_node_for_obj(lpid, ip_hint);
  if (unlikely(!node_optional)) {
    return std::nullopt;
  }
  auto &node = *node_optional;
  auto [iter, _] = objs_map_.try_emplace(id);
  netaddr addr{node.ip, node.rpc_srv_port};
  iter->second = addr;
  return std::make_pair(id, addr);
}

void Controller::destroy_obj(RemObjID id) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = objs_map_.find(id);
  if (unlikely(iter == objs_map_.end())) {
    WARN();
    return;
  }
  free_heap_segments_.push(VAddrRange{id, id + kHeapSize});
  objs_map_.erase(iter);
}

std::optional<netaddr> Controller::resolve_obj(RemObjID id) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = objs_map_.find(id);
  if (unlikely(iter == objs_map_.end())) {
    return std::nullopt;
  } else {
    auto addr = iter->second;
    return addr;
  }
}

std::optional<Node> Controller::select_node_for_obj(lpid_t lpid,
                                                    uint32_t ip_hint) {
  auto &[nodes, rr_iter] = lpid_to_info_[lpid];
  BUG_ON(nodes.empty());

  if (ip_hint) {
    Node n{ip_hint, 0};
    auto iter = nodes.find(n);
    if (unlikely(iter == nodes.end())) {
      return std::nullopt;
    }
    return *iter;
  }

  // TODO: adopt a more sophisticated mechanism once we've added more fields.
  if (unlikely(rr_iter == nodes.end())) {
    rr_iter = nodes.begin();
  }
  return *rr_iter++;
}

std::optional<netaddr> Controller::get_migration_dest(lpid_t lpid,
                                                      uint32_t requestor_ip,
                                                      Resource resource) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto &nodes = lpid_to_info_[lpid].nodes;
  // TODO: choose the dest node based on resource requirement.
  for (auto &node : nodes) {
    if (node.ip != requestor_ip) {
      return netaddr{node.ip, node.migrator_port};
    }
  }
  return std::nullopt;
}

void Controller::update_location(RemObjID id, netaddr obj_srv_addr) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = objs_map_.find(id);
  BUG_ON(iter == objs_map_.end());
  iter->second = obj_srv_addr;
}

} // namespace nu
