#pragma once

#include <cstdint>
#include <list>
#include <optional>
#include <stack>
#include <unordered_map>
#include <utility>

extern "C" {
#include <runtime/net.h>
#include <runtime/tcp.h>
}
#include "sync.h"

#include "defs.hpp"
#include "heap_mgr.hpp"

namespace nu {

struct VAddrRange {
  uint64_t start;
  uint64_t end;
};

struct Node {
  netaddr addr;
  // TODO: add other informations, e.g., free mem size.
};

struct NodeWithConn {
  Node node;
  tcpconn_t *obj_srv_conn;
};

class Controller {
public:
  constexpr static uint64_t kMinVAddr = 0x40000000ULL;
  constexpr static uint64_t kMaxVAddr = 0x500000000000ULL;
  constexpr static uint64_t kMaxNumObjs =
      (kMaxVAddr - kMinVAddr) / HeapManager::kHeapSize;

  Controller();
  ~Controller();
  void register_node(Node node);
  std::optional<std::pair<RemObjID, VAddrRange>> allocate_obj();
  void destroy_obj(RemObjID id);
  std::optional<netaddr> resolve_obj(RemObjID id);

private:
  std::stack<VAddrRange> free_ranges_;
  std::unordered_map<RemObjID, std::pair<VAddrRange, netaddr>> objs_map_;
  std::list<NodeWithConn> nodes_;
  std::list<NodeWithConn>::iterator nodes_iter_;
  rt::Spin spin_;

  NodeWithConn select_node_for_obj();
  void construct_obj_on_node(void *heap_base, NodeWithConn node);
};
} // namespace nu
