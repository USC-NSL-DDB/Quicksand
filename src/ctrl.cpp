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

  nodes_iter_ = nodes_.end();
}

Controller::~Controller() {}

std::optional<std::pair<lpid_t, VAddrRange>>
Controller::register_node(Node &node, MD5Val md5) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  // TODO: should GC the allocated lpid and stack somehow through heartbeat or an
  // explicit deregister_node() call.
  if (node.lpid) {
    auto iter = free_lpids_.find(node.lpid);
    if (iter == free_lpids_.end()) {
      if (unlikely(lpid_to_md5_[node.lpid] != md5)) {
        return std::nullopt;
      }
    } else {
      free_lpids_.erase(iter);
      lpid_to_md5_[node.lpid] = md5;
    }
  } else {
    if (unlikely(free_lpids_.empty())) {
      return std::nullopt;
    }
    auto begin_iter = free_lpids_.begin();
    node.lpid = *begin_iter;
    free_lpids_.erase(begin_iter);
  }

  if (unlikely(free_stack_cluster_segments_.empty())) {
    free_lpids_.insert(node.lpid);
    return std::nullopt;
  }

  auto stack_cluster = free_stack_cluster_segments_.top();
  free_stack_cluster_segments_.pop();

  for (auto old_node : nodes_) {
    auto *client = Runtime::rpc_client_mgr->get_by_ip(old_node.ip);
    RPCReqReserveConns req;
    RPCReturnBuffer return_buf;
    req.dest_server_addr = netaddr{node.ip, node.migrator_port};
    BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
  }

  auto *client = Runtime::rpc_client_mgr->get_by_ip(node.ip);
  for (auto old_node : nodes_) {
    RPCReqReserveConns req;
    RPCReturnBuffer return_buf;
    req.dest_server_addr = netaddr{old_node.ip, old_node.migrator_port};
    BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
  }

  nodes_.insert(node);
  return std::make_pair(node.lpid, stack_cluster);
}

bool Controller::verify_md5(lpid_t lpid, MD5Val md5) {
  return lpid_to_md5_[lpid] == md5;
}

std::optional<std::pair<RemObjID, netaddr>>
Controller::allocate_obj(netaddr hint) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  if (unlikely(free_heap_segments_.empty())) {
    return std::nullopt;
  }
  auto start_addr = free_heap_segments_.top().start;
  auto id = start_addr;
  free_heap_segments_.pop();
  auto node_optional = select_node_for_obj(hint);
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

std::optional<Node> Controller::select_node_for_obj(netaddr hint) {
  BUG_ON(nodes_.empty());

  if (hint.ip) {
    Node n{hint.ip, hint.port};
    auto iter = nodes_.find(n);
    if (unlikely(iter == nodes_.end())) {
      return std::nullopt;
    }
    return *iter;
  }

  // TODO: adopt a more sophisticated mechanism once we've added more fields.
  if (unlikely(nodes_iter_ == nodes_.end())) {
    nodes_iter_ = nodes_.begin();
  }
  return *nodes_iter_++;
}

std::optional<netaddr> Controller::get_migration_dest(uint32_t requestor_ip,
                                                      Resource resource) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);
  // TODO: choose the dest node based on resource requirement.
  for (auto &node : nodes_) {
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
