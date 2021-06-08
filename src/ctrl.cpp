#include <cereal/archives/binary.hpp>
#include <cstdint>

extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
#include <net/ip.h>
}
#include "thread.h"

#include "ctrl.hpp"
#include "obj_server.hpp"

namespace nu {

Controller::Controller() {
  for (uint64_t vaddr = kMinVAddr; vaddr + HeapManager::kHeapSize <= kMaxVAddr;
       vaddr += HeapManager::kHeapSize) {
    VAddrRange range = {.start = vaddr, .end = vaddr + HeapManager::kHeapSize};
    free_ranges_.push(range);
  }
  nodes_iter_ = nodes_.end();
}

Controller::~Controller() {
  for (auto &node : nodes_) {
    auto *c = node.migrator_conn;
    BUG_ON(c->Shutdown(SHUT_RDWR) < 0);
    delete c;
  }
}

void Controller::register_node(Node &node) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  for (auto old_node : nodes_) {
    auto migrator_conn = old_node.migrator_conn;
    uint8_t type = RESERVE_CONNS;
    RPCReqReserveConns req;
    req.num = Migrator::kDefaultNumReservedConns;
    req.dest_server_addr = node.migrator_addr;
    const iovec iovecs[] = {{&type, sizeof(type)}, {&req, sizeof(req)}};
    BUG_ON(migrator_conn->WritevFull(std::span(iovecs)) < 0);
  }

  netaddr local_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
  node.migrator_conn = rt::TcpConn::Dial(local_addr, node.migrator_addr);
  BUG_ON(!node.migrator_conn);

  for (auto old_node : nodes_) {
    uint8_t type = RESERVE_CONNS;
    RPCReqReserveConns req;
    req.num = Migrator::kDefaultNumReservedConns;
    req.dest_server_addr = old_node.migrator_addr;
    const iovec iovecs[] = {{&type, sizeof(type)}, {&req, sizeof(req)}};
    BUG_ON(node.migrator_conn->WritevFull(std::span(iovecs)) < 0);
  }

  nodes_.insert(node);
}

std::optional<std::pair<RemObjID, VAddrRange>>
Controller::allocate_obj(std::optional<netaddr> hint) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  if (unlikely(free_ranges_.empty())) {
    return std::nullopt;
  }
  auto range = free_ranges_.top();
  auto id = range.start;
  free_ranges_.pop();
  auto node_optional = select_node_for_obj(hint);
  if (unlikely(!node_optional)) {
    return std::nullopt;
  }
  objs_map_.emplace(id, std::make_pair(range, node_optional->obj_srv_addr));
  return std::make_pair(id, range);
}

void Controller::destroy_obj(RemObjID id) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = objs_map_.find(id);
  if (unlikely(iter == objs_map_.end())) {
    WARN();
    return;
  }
  auto &p = iter->second;
  auto &range = p.first;
  free_ranges_.push(range);
  objs_map_.erase(iter);
}

std::optional<netaddr> Controller::resolve_obj(RemObjID id) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = objs_map_.find(id);
  if (unlikely(iter == objs_map_.end())) {
    return std::nullopt;
  } else {
    auto &p = iter->second;
    auto &addr = p.second;
    return addr;
  }
}

std::optional<Node>
Controller::select_node_for_obj(std::optional<netaddr> hint) {
  BUG_ON(nodes_.empty());

  if (hint) {
    Node n;
    n.obj_srv_addr = *hint;
    auto iter = nodes_.find(n);
    if (unlikely(iter == nodes_.end())) {
      return std::nullopt;
    }
    return *iter;
  }

  // TODO: adopt a more sophisticated mechanism once we've added more fields to
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
    if (node.migrator_addr.ip != requestor_ip) {
      return node.migrator_addr;
    }
  }
  return std::nullopt;
}

void Controller::update_location(RemObjID id, netaddr obj_srv_addr) {
  rt::ScopedLock<rt::Mutex> lock(&mutex_);

  auto iter = objs_map_.find(id);
  BUG_ON(iter == objs_map_.end());
  iter->second.second = obj_srv_addr;
}

} // namespace nu
