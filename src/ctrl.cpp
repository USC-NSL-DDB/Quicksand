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

  done_ = false;
}

Controller::~Controller() {
  done_ = true;
  barrier();
  for (auto &th : probing_threads_) {
    th.Join();
  }
}

std::optional<std::pair<lpid_t, VAddrRange>>
Controller::register_node(Node &node, lpid_t lpid, MD5Val md5) {
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
  probing_threads_.emplace_back(create_probing_thread(nodes, iter));
  return std::make_pair(lpid, stack_cluster);
}

bool Controller::verify_md5(lpid_t lpid, MD5Val md5) {
  if constexpr (kEnableBinaryVerification) {
    return lpid_to_md5_[lpid] == md5;
  } else {
    return true;
  }
}

std::optional<std::pair<RemObjID, uint32_t>>
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
  auto [iter, _] = obj_id_to_ip_.try_emplace(id);
  iter->second = node.ip;
  return std::make_pair(id, node.ip);
}

void Controller::destroy_obj(RemObjID id) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = obj_id_to_ip_.find(id);
  if (unlikely(iter == obj_id_to_ip_.end())) {
    WARN();
    return;
  }
  free_heap_segments_.push(VAddrRange{id, id + kHeapSize});
  obj_id_to_ip_.erase(iter);
}

uint32_t Controller::resolve_obj(RemObjID id) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = obj_id_to_ip_.find(id);
  return iter != obj_id_to_ip_.end() ? iter->second : 0;
}

std::optional<Node> Controller::select_node_for_obj(lpid_t lpid,
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

void Controller::update_location(RemObjID id, uint32_t obj_srv_ip) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = obj_id_to_ip_.find(id);
  BUG_ON(iter == obj_id_to_ip_.end());
  iter->second = obj_srv_ip;
}

rt::Thread Controller::create_probing_thread(std::set<Node> &nodes,
                                             std::set<Node>::iterator iter) {
  return rt::Thread([&, iter] {
    bool node_alive;
    while (!rt::access_once(done_) && (node_alive = update_node(iter))) {
      timer_sleep(kProbingIntervalUs);
    }
    if (!node_alive) {
      rt::MutexGuard g(&mutex_);
      nodes.erase(iter);
    }
  });
}

bool Controller::update_node(std::set<Node>::iterator iter) {
  auto *client = Runtime::rpc_client_mgr->get_by_ip(iter->ip);
  RPCReqProbeFreeResource req;
  RPCReturnBuffer return_buf;
  auto alive = (client->Call(to_span(req), &return_buf) == kOk);
  if (alive) {
    auto &resp = from_span<RPCRespProbeFreeResource>(return_buf.get_buf());
    const_cast<Resource &>(iter->free_resource) = resp.resource;
    return true;
  } else {
    return false;
  }
}

} // namespace nu
