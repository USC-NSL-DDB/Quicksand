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
  // TODO: add other informations, e.g., free mem size.
  netaddr obj_srv_addr;
  netaddr migrator_addr;
  tcpconn_t *migrator_conn;
};

class Controller {
public:
  constexpr static uint64_t kMinVAddr = 0x40000000ULL;
  constexpr static uint64_t kMaxVAddr = 0x500000000000ULL;
  constexpr static uint64_t kMaxNumObjs =
      (kMaxVAddr - kMinVAddr) / HeapManager::kHeapSize;

  Controller();
  ~Controller();
  void register_node(Node &node);
  std::optional<std::pair<RemObjID, VAddrRange>> allocate_obj();
  void destroy_obj(RemObjID id);
  std::optional<netaddr> resolve_obj(RemObjID id);
  std::optional<netaddr> get_migration_dest(uint32_t requestor_ip,
                                            Resource resource);
  void update_location(RemObjID id, netaddr obj_srv_addr);

private:
  std::stack<VAddrRange> free_ranges_;
  std::unordered_map<RemObjID, std::pair<VAddrRange, netaddr>> objs_map_;
  std::list<Node> nodes_;
  std::list<Node>::iterator nodes_iter_;
  rt::Mutex mutex_;

  Node select_node_for_obj();
};
} // namespace nu
