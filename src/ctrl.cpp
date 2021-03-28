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

void Controller::register_node(const Node &node) {
  rt::ScopedLock<rt::Spin> lock(&spin_);
  nodes_.push_back(node);
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
  objs_map_.emplace(id, std::make_pair(range, node.obj_srv_addr));
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

Node Controller::select_node_for_obj() {
  // TODO: adopt a more sophisticated mechanism once we've added more fields to
  BUG_ON(nodes_.empty());
  if (unlikely(nodes_iter_ == nodes_.end())) {
    nodes_iter_ = nodes_.begin();
  }
  return *nodes_iter_++;
}

std::optional<netaddr> Controller::get_migration_dest(uint32_t requestor_ip,
                                                      Resource resource) {
  rt::ScopedLock<rt::Spin> lock(&spin_);
  // TODO: choose the dest node based on resource requirement.
  for (auto &node : nodes_) {
    if (node.migra_ldr_addr.ip != requestor_ip) {
      return node.migra_ldr_addr;
    }
  }
  return std::nullopt;
}

void Controller::update_location(RemObjID id, netaddr obj_srv_addr) {
  rt::ScopedLock<rt::Spin> lock(&spin_);

  auto iter = objs_map_.find(id);
  BUG_ON(iter == objs_map_.end());
  iter->second.second = obj_srv_addr;
}

} // namespace nu
