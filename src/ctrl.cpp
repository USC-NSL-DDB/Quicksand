#include <cereal/archives/binary.hpp>
#include <cstdint>
#include <limits>

extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
#include <net/ip.h>
#include <runtime/timer.h>
}
#include <thread.h>

#include "nu/commons.hpp"
#include "nu/ctrl.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/migrator.hpp"
#include "nu/proclet_server.hpp"

namespace nu {

Controller::Controller() {
  for (lpid_t lpid = 1; lpid < std::numeric_limits<lpid_t>::max(); lpid++) {
    free_lpids_.insert(lpid);
  }

  for (uint64_t start_addr = kMinProcletHeapVAddr;
       start_addr + kProcletHeapSize <= kMaxProcletHeapVAddr;
       start_addr += kProcletHeapSize) {
    VAddrRange range = {.start = start_addr,
                        .end = start_addr + kProcletHeapSize};
    free_proclet_heap_segments_.push(range);
  }

  for (uint64_t start_addr = kMinStackClusterVAddr;
       start_addr + kStackClusterSize <= kMaxStackClusterVAddr;
       start_addr += kStackClusterSize) {
    VAddrRange range = {.start = start_addr,
                        .end = start_addr + kStackClusterSize};
    free_stack_cluster_segments_.push(range);
  }

  done_ = false;
}

Controller::~Controller() {
  done_ = true;
  barrier();
}

std::optional<std::pair<lpid_t, VAddrRange>> Controller::register_node(
    Node &node, lpid_t lpid, MD5Val md5) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  // TODO: should GC the allocated lpid and stack somehow through heartbeat or
  // an explicit deregister_node() call.
  if (lpid) {
    auto iter = free_lpids_.find(lpid);
    if (iter == free_lpids_.end()) {
      if constexpr (kEnableBinaryVerification) {
        if (unlikely(lpid_to_md5_[lpid] != md5)) {
          return std::nullopt;
        }
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
    req.dest_server_ip = node.ip;
    BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
  }

  auto *client = Runtime::rpc_client_mgr->get_by_ip(node.ip);
  for (auto old_node : nodes) {
    RPCReqReserveConns req;
    RPCReturnBuffer return_buf;
    req.dest_server_ip = old_node.ip;
    BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
  }

  auto [iter, success] = nodes.insert(node);
  BUG_ON(!success);
  return std::make_pair(lpid, stack_cluster);
}

bool Controller::verify_md5(lpid_t lpid, MD5Val md5) {
  if constexpr (kEnableBinaryVerification) {
    return lpid_to_md5_[lpid] == md5;
  } else {
    return true;
  }
}

std::optional<std::pair<ProcletID, uint32_t>> Controller::allocate_proclet(
    lpid_t lpid, uint32_t ip_hint) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  if (unlikely(free_proclet_heap_segments_.empty())) {
    return std::nullopt;
  }
  auto start_addr = free_proclet_heap_segments_.top().start;
  auto id = start_addr;
  free_proclet_heap_segments_.pop();
  auto node_optional = select_node_for_proclet(lpid, ip_hint);
  if (unlikely(!node_optional)) {
    return std::nullopt;
  }
  auto &node = *node_optional;
  auto [iter, _] = proclet_id_to_ip_.try_emplace(id);
  iter->second = node.ip;
  return std::make_pair(id, node.ip);
}

void Controller::destroy_proclet(ProcletID id) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = proclet_id_to_ip_.find(id);
  if (unlikely(iter == proclet_id_to_ip_.end())) {
    WARN();
    return;
  }
  auto start_addr = id;
  auto end_addr = start_addr + kProcletHeapSize;
  free_proclet_heap_segments_.push(VAddrRange{start_addr, end_addr});
  proclet_id_to_ip_.erase(iter);
}

uint32_t Controller::resolve_proclet(ProcletID id) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = proclet_id_to_ip_.find(id);
  return iter != proclet_id_to_ip_.end() ? iter->second : 0;
}

std::optional<Node> Controller::select_node_for_proclet(lpid_t lpid,
                                                        uint32_t ip_hint) {
  auto &[nodes, rr_iter] = lpid_to_info_[lpid];
  BUG_ON(nodes.empty());

  if (ip_hint) {
    Node n{ip_hint};
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

uint32_t Controller::get_migration_dest(lpid_t lpid, uint32_t requestor_ip,
                                        Resource resource) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto &[nodes, rr_iter] = lpid_to_info_[lpid];
  auto initial_rr_iter = rr_iter;
  BUG_ON(nodes.empty());

again:
  if (unlikely(rr_iter == nodes.end())) {
    rr_iter = nodes.begin();
  }

  if (rr_iter->ip == requestor_ip || !rr_iter->has_enough_resource(resource)) {
    rr_iter++;
    if (unlikely(rr_iter == initial_rr_iter)) {
      return 0;
    }
    goto again;
  }

  return rr_iter++->ip;
}

void Controller::update_location(ProcletID id, uint32_t proclet_srv_ip) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = proclet_id_to_ip_.find(id);
  BUG_ON(iter == proclet_id_to_ip_.end());
  iter->second = proclet_srv_ip;
}

void Controller::report_free_resource(lpid_t lpid, uint32_t ip,
                                      Resource free_resource) {
  auto lp_info_iter = lpid_to_info_.find(lpid);
  BUG_ON(lp_info_iter == lpid_to_info_.end());

  auto &nodes = lp_info_iter->second.nodes;
  Node node{.ip = ip};
  auto node_iter = nodes.find(node);
  BUG_ON(node_iter == nodes.end());

  const_cast<Node &>(*node_iter).update_free_resource(free_resource);
}

template <typename T>
void ewma(double weight, T *result, T new_data) {
  *result = *result * weight + (1 - weight) * new_data;
}

void Node::update_free_resource(Resource resource) {
  constexpr double kWeight = 0.8;
  ewma(kWeight, &free_resource.cores, resource.cores);
  ewma(kWeight, &free_resource.mem_mbs, resource.mem_mbs);
}

}  // namespace nu
