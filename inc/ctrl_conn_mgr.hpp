#pragma once

#include <functional>

extern "C" {
#include <runtime/net.h>
}

#include "conn_mgr.hpp"
#include "defs.hpp"

namespace nu {

class ControllerConnManager {
public:
  ControllerConnManager(netaddr remote_ctrl_addr);
  tcpconn_t *get_conn();
  void put_conn(tcpconn_t *conn);
private:
  std::function<tcpconn_t *(bool unused)> creator_;
  ConnectionManager<bool> mgr_;
};

} // namespace nu

#include "impl/ctrl_conn_mgr.ipp"
