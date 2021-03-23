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
#include "utils/tcp.hpp"

namespace nu {

Controller::Controller() {
  for (uint64_t vaddr = kMinVAddr; vaddr < kMaxVAddr;
       vaddr += HeapManager::kHeapSize) {
    VAddrRange range = {.start = vaddr, .end = vaddr + HeapManager::kHeapSize};
    free_ranges_.push(range);
  }
  nodes_iter_ = nodes_.end();
}

Controller::~Controller() {}

void Controller::register_node(Node node) {
  NodeWithConn node_with_conn;
  node_with_conn.node = node;
  netaddr laddr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
  BUG_ON(tcp_dial(laddr, node.addr, &node_with_conn.obj_srv_conn));

  rt::ScopedLock<rt::Spin> lock(&spin_);
  nodes_.push_back(node_with_conn);
}

std::optional<std::pair<RemObjID, VAddrRange>> Controller::allocate_obj() {
  rt::ScopedLock<rt::Spin> lock(&spin_);

  if (unlikely(free_ranges_.empty())) {
    return std::nullopt;
  }
  auto range = free_ranges_.top();
  auto id = range.start;
  free_ranges_.pop();
  auto node = select_node_for_obj();
  objs_map_.emplace(id, std::make_pair(range, node.node.addr));
  return std::make_pair(id, range);
}

void Controller::destroy_obj(RemObjID id) {
  rt::ScopedLock<rt::Spin> lock(&spin_);

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
  rt::ScopedLock<rt::Spin> lock(&spin_);

  auto iter = objs_map_.find(id);
  if (unlikely(iter == objs_map_.end())) {
    return std::nullopt;
  } else {
    auto &p = iter->second;
    auto &addr = p.second;
    return addr;
  }
}

NodeWithConn Controller::select_node_for_obj() {
  // TODO: adopt a more sophisticated mechanism once we've added more fields to
  BUG_ON(nodes_.empty());
  if (unlikely(nodes_iter_ == nodes_.end())) {
    nodes_iter_ = nodes_.begin();
  }
  return *nodes_iter_++;
}

} // namespace nu
