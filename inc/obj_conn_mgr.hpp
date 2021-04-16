#pragma once

#include <functional>

extern "C" {
#include <runtime/net.h>
}
#include <sync.h>

#include "conn_mgr.hpp"
#include "defs.hpp"
#include "utils/rcu_hash_map.hpp"
#include "utils/netaddr.hpp"

namespace nu {

class RemObjConnManager {
public:
  constexpr static uint32_t kNumPerCoreCachedConns = 8;
  constexpr static uint32_t kUpdateAddrRetryUs = 100;

  RemObjConnManager();
  tcpconn_t *get_conn(RemObjID id);
  void put_conn(tcpconn_t *conn);
  void update_addr(RemObjID id);
  void reserve_conns(uint32_t num, netaddr obj_server_addr);

private:
  RCUHashMap<RemObjID, netaddr> id_map_;
  ConnectionManager<netaddr> mgr_;

  netaddr get_addr(RemObjID id);
};

} // namespace nu

#include "impl/obj_conn_mgr.ipp"
