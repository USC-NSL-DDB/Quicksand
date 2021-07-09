#pragma once

#include <functional>

extern "C" {
#include <runtime/net.h>
}
#include <net.h>
#include <sync.h>

#include "nu/conn_mgr.hpp"
#include "nu/commons.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/utils/netaddr.hpp"
#include "nu/utils/rcu_hash_map.hpp"

namespace nu {

class RemObjConnManager {
public:
  constexpr static uint32_t kNumPerCoreCachedConns = 8;

  RemObjConnManager();
  rt::TcpConn *get_conn(RemObjID id);
  void put_conn(rt::TcpConn *conn);
  void update_addr(RemObjID id);
  void reserve_conns(uint32_t num, netaddr obj_server_addr);

private:
  RCUHashMap<RemObjID, netaddr,
             RuntimeAllocator<std::pair<const RemObjID, netaddr>>>
      id_map_;
  ConnectionManager<netaddr> mgr_;

  netaddr get_addr(RemObjID id);
};

} // namespace nu

#include "nu/impl/obj_conn_mgr.ipp"
