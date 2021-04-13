#pragma once

#include <functional>

extern "C" {
#include <runtime/net.h>
}
#include <sync.h>

#include "conn_mgr.hpp"
#include "defs.hpp"
#include "utils/ts_hash_map.hpp"
#include "utils/netaddr.hpp"

namespace nu {

class RemObjConnManager {
public:
  constexpr static uint32_t kNumPerCoreCachedConns = 4;
  constexpr static uint32_t kUpdateAddrRetryUs = 100;

  RemObjConnManager();
  tcpconn_t *get_conn(RemObjID id);
  void put_conn(tcpconn_t *conn);
  void update_addr(RemObjID id, netaddr old_addr);
  void reserve_conns(uint32_t num, netaddr obj_server_addr);

private:
  struct IDMapEntry {
    union Addr {
      netaddr addr;
      uint64_t raw;
    } addr;
    bool valid = false;
    rt::Mutex mutex;
  };
  static_assert(sizeof(IDMapEntry::Addr) == 8);

  ThreadSafeHashMap<RemObjID, IDMapEntry> id_map_;
  ConnectionManager<netaddr> mgr_;

  netaddr get_addr(RemObjID id);
};

} // namespace nu

#include "impl/obj_conn_mgr.ipp"
