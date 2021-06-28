#pragma once

#include <functional>
#include <optional>

extern "C" {
#include <net/ip.h>
#include <runtime/net.h>
}
#include <net.h>
#include <sync.h>

#include "conn_mgr.hpp"
#include "ctrl_server.hpp"
#include "runtime.hpp"

namespace nu {

class ControllerConnManager {
public:
  constexpr static uint32_t kNumPerCoreCachedConns = 1;

  ControllerConnManager(uint32_t ctrl_server_ip);
  rt::TcpConn *get_conn();
  void put_conn(rt::TcpConn *conn);
  void reserve_conns(uint32_t num);

private:
  std::function<rt::TcpConn *(bool unused)> creator_;
  ConnectionManager<bool> mgr_;
};

class ControllerClient {
public:
  ControllerClient(uint32_t ctrl_server_ip, Runtime::Mode mode);
  VAddrRange register_node(const Node &node);
  std::optional<std::pair<RemObjID, netaddr>>
  allocate_obj(std::optional<netaddr> hint);
  void destroy_obj(RemObjID id);
  std::optional<netaddr> resolve_obj(RemObjID id);
  std::optional<netaddr> get_migration_dest(Resource resource);
  void update_location(RemObjID id, netaddr obj_srv_addr);
  void reserve_conns(uint32_t num);
  VAddrRange get_stack_cluster() const;

private:
  ControllerConnManager conn_mgr_;
  VAddrRange stack_cluster_;
};
} // namespace nu
