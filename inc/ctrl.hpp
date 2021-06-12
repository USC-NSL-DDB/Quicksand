#pragma once

#include <cstdint>
#include <list>
#include <optional>
#include <set>
#include <stack>
#include <unordered_map>
#include <utility>

extern "C" {
#include <runtime/net.h>
#include <runtime/tcp.h>
}
#include <net.h>
#include <sync.h>

#include "defs.hpp"
#include "heap_mgr.hpp"
#include "utils/netaddr.hpp"

namespace nu {

struct VAddrRange {
  uint64_t start;
  uint64_t end;
};

struct Node {
  // TODO: add other informations, e.g., free mem size.
  netaddr obj_srv_addr;
  netaddr migrator_addr;
  rt::TcpConn *migrator_conn;

  bool operator<(const Node &o) const { return obj_srv_addr < o.obj_srv_addr; }
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
  std::optional<std::pair<RemObjID, netaddr>>
  allocate_obj(std::optional<netaddr> hint);
  void destroy_obj(RemObjID id);
  std::optional<netaddr> resolve_obj(RemObjID id);
  std::optional<netaddr> get_migration_dest(uint32_t requestor_ip,
                                            Resource resource);
  void update_location(RemObjID id, netaddr obj_srv_addr);

private:
  std::stack<VAddrRange> free_ranges_;
  std::unordered_map<RemObjID, std::pair<VAddrRange, netaddr>> objs_map_;
  std::set<Node> nodes_;
  std::set<Node>::iterator nodes_iter_;
  rt::Mutex mutex_;

  std::optional<Node> select_node_for_obj(std::optional<netaddr> hint);
};
} // namespace nu
