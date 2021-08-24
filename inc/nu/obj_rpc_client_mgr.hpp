#pragma once

#include <functional>

extern "C" {
#include <runtime/net.h>
}
#include <net.h>
#include <sync.h>

#include "nu/commons.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/utils/netaddr.hpp"
#include "nu/utils/rpc_client_mgr.hpp"
#include <folly/concurrency/ConcurrentHashMap.h>

namespace nu {

class RemObjRPCClientMgr {
public:
  RemObjRPCClientMgr();
  RPCClient *get(RemObjID id);
  void update_addr(RemObjID id);

private:
  using Key = RemObjID;
  using Val = netaddr;
  using Hash = std::hash<Key>;
  using Equal = std::equal_to<Key>;
  using Allocator = RuntimeAllocator<uint8_t>;

  folly::ConcurrentHashMap<Key, Val, Hash, Equal, Allocator> id_map_;
  RPCClientMgr<netaddr, RuntimeAllocator<uint8_t>> mgr_;

  netaddr get_addr(RemObjID id);
};

} // namespace nu

#include "nu/impl/obj_rpc_client_mgr.ipp"
