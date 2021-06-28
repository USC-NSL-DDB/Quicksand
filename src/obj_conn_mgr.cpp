extern "C" {
#include <net/ip.h>
#include <runtime/tcp.h>
}

#include "nu/ctrl_client.hpp"
#include "nu/obj_conn_mgr.hpp"
#include "nu/runtime.hpp"

namespace nu {

RemObjConnManager::RemObjConnManager()
    : mgr_(
          [](netaddr server_addr) {
            netaddr local_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
            auto *heap = Runtime::get_heap();
            Runtime::switch_to_runtime_heap();
            auto c = rt::TcpConn::Dial(local_addr, server_addr);
            Runtime::set_heap(heap);
            BUG_ON(!c);
            return c;
          },
          kNumPerCoreCachedConns) {}

netaddr RemObjConnManager::get_addr(RemObjID id) {
  auto *addr = id_map_.get(id);

  if (unlikely(!addr)) {
    auto optional_addr = Runtime::controller_client->resolve_obj(id);
    BUG_ON(!optional_addr);
    id_map_.put_if_not_exists(id, *optional_addr);
    return *optional_addr;
  }

  return *addr;
}

void RemObjConnManager::update_addr(RemObjID id) { id_map_.remove(id); }

} // namespace nu
