extern "C" {
#include <net/ip.h>
}

#include "runtime.hpp"

namespace nu {

inline ControllerConnManager::ControllerConnManager(netaddr remote_ctrl_addr)
    : creator_([remote_ctrl_addr](bool unused) {
        tcpconn_t *tcp_conn;
        netaddr local_ctrl_client_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0),
                                          .port = 0};
        BUG_ON(tcp_dial(local_ctrl_client_addr, remote_ctrl_addr, &tcp_conn) !=
               0);
        return tcp_conn;
      }),
      mgr_(creator_) {}

inline tcpconn_t *ControllerConnManager::get_conn() {
  return mgr_.get_conn(false);
}

inline void ControllerConnManager::put_conn(tcpconn_t *conn) {
  mgr_.put_conn(false, conn);
}
} // namespace nu
