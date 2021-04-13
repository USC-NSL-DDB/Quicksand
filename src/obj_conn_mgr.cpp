extern "C" {
#include <net/ip.h>
#include <runtime/tcp.h>
}

#include "ctrl_client.hpp"
#include "obj_conn_mgr.hpp"
#include "runtime.hpp"

namespace nu {

RemObjConnManager::RemObjConnManager()
    : mgr_(
          [](netaddr server_addr) {
            tcpconn_t *c;
            netaddr local_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
            BUG_ON(tcp_dial(local_addr, server_addr, &c) != 0);
            return c;
          },
          kNumPerCoreCachedConns) {}

netaddr RemObjConnManager::get_addr(RemObjID id) {
  auto &id_map_entry = id_map_.get_or_emplace(id);
  if (unlikely(!load_acquire(&id_map_entry.valid))) {
    rt::ScopedLock<rt::Mutex> lock(&id_map_entry.mutex);
    if (!id_map_entry.valid) {
      auto optional_addr = Runtime::controller_client->resolve_obj(id);
      BUG_ON(!optional_addr);
      IDMapEntry::Addr addr;
      addr.addr = *optional_addr;
      id_map_entry.addr.raw = addr.raw; // Guaranteed to be atomic.
      store_release(&id_map_entry.valid, true);
    }
  }
  return id_map_entry.addr.addr;
}

void RemObjConnManager::update_addr(RemObjID id, netaddr old_addr) {
  auto &id_map_entry = id_map_.get(id);
  if (id_map_entry.addr.addr == old_addr) {
    rt::ScopedLock<rt::Mutex> lock(&id_map_entry.mutex);
    if (likely(id_map_entry.addr.addr == old_addr)) {
      id_map_entry.valid = false;
    retry:
      auto optional_addr = Runtime::controller_client->resolve_obj(id);
      BUG_ON(!optional_addr);
      if (unlikely(*optional_addr == old_addr)) {
        timer_sleep(kUpdateAddrRetryUs);
        goto retry;
      }
      IDMapEntry::Addr addr;
      addr.addr = *optional_addr;
      id_map_entry.addr.raw = addr.raw; // Guaranteed to be atomic.
      store_release(&id_map_entry.valid, true);
    }
  }
}

} // namespace nu


