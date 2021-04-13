#pragma once

#include <functional>

extern "C" {
#include <net/ip.h>
#include <runtime/net.h>
}
#include "sync.h"

#include "conn_mgr.hpp"
#include "ctrl_server.hpp"

namespace nu {

class ControllerConnManager {
public:
  constexpr static uint32_t kNumPerCoreCachedConns = 1;

  ControllerConnManager(netaddr remote_ctrl_addr);
  tcpconn_t *get_conn();
  void put_conn(tcpconn_t *conn);
  void reserve_conns(uint32_t num);

private:
  std::function<tcpconn_t *(bool unused)> creator_;
  ConnectionManager<bool> mgr_;
};

class ControllerClient {
public:
  ControllerClient(netaddr remote_ctrl_addr);
  ControllerClient(uint16_t local_obj_srv_port, uint16_t local_migra_ldr_port,
                   netaddr remote_ctrl_addr);
  void register_node(const Node &node);
  std::optional<std::pair<RemObjID, VAddrRange>> allocate_obj();
  void destroy_obj(RemObjID id);
  std::optional<netaddr> resolve_obj(RemObjID id);
  std::optional<netaddr> get_migration_dest(Resource resource);
  void update_location(RemObjID id, netaddr obj_srv_addr);
  void reserve_conns(uint32_t num);

private:
  ControllerConnManager conn_mgr_;
};
} // namespace nu

