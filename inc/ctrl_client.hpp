#pragma once

#include "sync.h"

#include "ctrl_conn_mgr.hpp"
#include "ctrl_server.hpp"

namespace nu {

class ControllerConnManager;

class ControllerClient {
public:
  ControllerClient();
  ControllerClient(uint16_t local_obj_srv_port, netaddr remote_ctrl_addr,
                   bool server_mode);
  void register_node(Node node);
  std::optional<std::pair<RemObjID, VAddrRange>> allocate_obj();
  void destroy_obj(RemObjID id);
  std::optional<netaddr> resolve_obj(RemObjID id);

private:
  std::unique_ptr<ControllerConnManager> conn_mgr_;
};
} // namespace nu
