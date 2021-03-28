extern "C" {
#include <net/ip.h>
#include <runtime/tcp.h>
}

#include "ctrl_client.hpp"
#include "obj_conn_mgr.hpp"
#include "runtime.hpp"

namespace nu {

std::function<tcpconn_t *(RemObjID)> RemObjConnManager::creator_ =
    [](RemObjID id) {
      auto optional = Runtime::controller_client->resolve_obj(id);
      BUG_ON(!optional);
      auto server_addr = *optional;

      tcpconn_t *c;
      netaddr local_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
      BUG_ON(tcp_dial(local_addr, server_addr, &c) != 0);
      return c;
    };

RemObjConnManager::RemObjConnManager()
    : ConnectionManager<RemObjID>(creator_) {}

} // namespace nu
