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
#include "nu/runtime.hpp"
#include "nu/ctrl.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/migrator.hpp"
#include "nu/proclet_server.hpp"

namespace nu {

constexpr NodeIP get_proclet_segment_bucket_id(uint64_t capacity) {
  auto bsr_capacity = bsr_64(capacity);
  auto bsr_min = bsr_64(kMinProcletHeapSize);
  BUG_ON(bsr_capacity < bsr_min);
  return bsr_capacity - bsr_min;
}

Controller::Controller() {
  for (lpid_t lpid = 1; lpid < std::numeric_limits<lpid_t>::max(); lpid++) {
    free_lpids_.insert(lpid);
  }

  auto &highest_bucket =
      free_proclet_heap_segments_[kNumProcletSegmentBuckets - 1];
  for (uint64_t start_addr = kMinProcletHeapVAddr;
       start_addr + kMaxProcletHeapSize <= kMaxProcletHeapVAddr;
       start_addr += kMaxProcletHeapSize) {
    VAddrRange range = {.start = start_addr,
                        .end = start_addr + kMaxProcletHeapSize};
    highest_bucket.push({range, 0});
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
    NodeIP ip, lpid_t lpid, MD5Val md5) {
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

  auto &node_statuses = lpid_to_info_[lpid].node_statuses;
  for (auto [existing_node_ip, _] : node_statuses) {
    auto *client = get_runtime()->rpc_client_mgr()->get_by_ip(existing_node_ip);
    RPCReqReserveConns req;
    RPCReturnBuffer return_buf;
    req.dest_server_ip = ip;
    BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
  }

  auto *client = get_runtime()->rpc_client_mgr()->get_by_ip(ip);
  for (auto [existing_node_ip, _] : node_statuses) {
    RPCReqReserveConns req;
    RPCReturnBuffer return_buf;
    req.dest_server_ip = existing_node_ip;
    BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
  }

  auto [iter, success] = node_statuses.try_emplace(ip, /* acquired = */ false);
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

std::optional<std::pair<ProcletID, NodeIP>> Controller::allocate_proclet(
    uint64_t capacity, lpid_t lpid, NodeIP ip_hint) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto &bucket =
      free_proclet_heap_segments_[get_proclet_segment_bucket_id(capacity)];
  if (unlikely(bucket.empty())) {
    auto &highest_bucket =
        free_proclet_heap_segments_[kNumProcletSegmentBuckets - 1];
    if (unlikely(highest_bucket.empty())) {
      return std::nullopt;
    }
    auto max_segment = highest_bucket.top();
    highest_bucket.pop();
    for (auto start_addr = max_segment.range.start;
         start_addr < max_segment.range.end; start_addr += capacity) {
      VAddrRange range = {.start = start_addr, .end = start_addr + capacity};
      bucket.push({range, max_segment.prev_host});
    }
  }

  auto segment = bucket.top();
  bucket.pop();
  auto start_addr = segment.range.start;
  auto id = start_addr;
  auto node_ip = select_node_for_proclet(lpid, ip_hint, segment);
  if (unlikely(!node_ip)) {
    return std::nullopt;
  }
  auto [iter, _] = proclet_id_to_ip_.try_emplace(id);
  iter->second = node_ip;
  return std::make_pair(id, node_ip);
}

void Controller::destroy_proclet(VAddrRange proclet_segment) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto capacity = proclet_segment.end - proclet_segment.start;
  auto &bucket =
      free_proclet_heap_segments_[get_proclet_segment_bucket_id(capacity)];
  auto proclet_id = proclet_segment.start;
  auto iter = proclet_id_to_ip_.find(proclet_id);
  if (unlikely(iter == proclet_id_to_ip_.end())) {
    WARN();
    return;
  }
  bucket.push({proclet_segment, iter->second});
  proclet_id_to_ip_.erase(iter);
}

NodeIP Controller::resolve_proclet(ProcletID id) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = proclet_id_to_ip_.find(id);
  return iter != proclet_id_to_ip_.end() ? iter->second : 0;
}

NodeIP Controller::select_node_for_proclet(lpid_t lpid, NodeIP ip_hint,
                                           const ProcletHeapSegment &segment) {
  auto &[node_statuses, rr_iter] = lpid_to_info_[lpid];
  BUG_ON(node_statuses.empty());

  if (ip_hint) {
    auto iter = node_statuses.find(ip_hint);
    if (unlikely(iter == node_statuses.end())) {
      return 0;
    }
    return ip_hint;
  }

  if (segment.prev_host) {
    return segment.prev_host;
  }

  // TODO: adopt a more sophisticated mechanism once we've added more fields.
  if (unlikely(rr_iter == node_statuses.end())) {
    rr_iter = node_statuses.begin();
  }
  return rr_iter++->first;
}

NodeIP Controller::acquire_migration_dest(lpid_t lpid, NodeIP requestor_ip,
                                          Resource resource) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto &[node_statuses, rr_iter] = lpid_to_info_[lpid];
  auto initial_rr_iter = rr_iter;
  BUG_ON(node_statuses.empty());

again:
  if (unlikely(rr_iter == node_statuses.end())) {
    rr_iter = node_statuses.begin();
  }

  if (rr_iter->first == requestor_ip || rr_iter->second.acquired ||
      !rr_iter->second.has_enough_resource(resource)) {
    rr_iter++;
    if (unlikely(rr_iter == initial_rr_iter)) {
      return 0;
    }
    goto again;
  }

  rr_iter->second.acquired = true;
  return rr_iter++->first;
}

void Controller::release_migration_dest(lpid_t lpid, NodeIP ip) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto &[node_statuses, _] = lpid_to_info_[lpid];
  auto iter = node_statuses.find(ip);
  if (unlikely(iter == node_statuses.end())) {
    return;
  }

  iter->second.acquired = false;
}

void Controller::update_location(ProcletID id, NodeIP proclet_srv_ip) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = proclet_id_to_ip_.find(id);
  BUG_ON(iter == proclet_id_to_ip_.end());
  iter->second = proclet_srv_ip;
}

void Controller::report_free_resource(lpid_t lpid, NodeIP ip,
                                      Resource free_resource) {
  auto lp_info_iter = lpid_to_info_.find(lpid);
  BUG_ON(lp_info_iter == lpid_to_info_.end());

  auto &node_statuses = lp_info_iter->second.node_statuses;
  auto iter = node_statuses.find(ip);
  BUG_ON(iter == node_statuses.end());

  iter->second.update_free_resource(free_resource);
}

void NodeStatus::update_free_resource(Resource resource) {
  ewma(kEWMAWeight, &free_resource.cores, resource.cores);
  ewma(kEWMAWeight, &free_resource.mem_mbs, resource.mem_mbs);
}

}  // namespace nu
