#pragma once

#include <functional>
#include <utility>

extern "C" {
#include <net/ip.h>
#include <runtime/net.h>
}
#include <net.h>
#include <sync.h>

#include "nu/commons.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/rpc_server.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/utils/rpc.hpp"

namespace nu {

// TODO: return heap back.

class ControllerClient {
public:
  ControllerClient(uint32_t ctrl_server_ip, Runtime::Mode mode, lpid_t lpid);
  std::optional<std::pair<lpid_t, VAddrRange>> register_node(const Node &node);
  std::optional<std::pair<RemObjID, netaddr>>
  allocate_obj(std::optional<netaddr> hint);
  void destroy_obj(RemObjID id);
  std::optional<netaddr> resolve_obj(RemObjID id);
  std::optional<netaddr> get_migration_dest(Resource resource);
  void update_location(RemObjID id, netaddr obj_srv_addr);
  VAddrRange get_stack_cluster() const;

private:
  lpid_t lpid_;
  VAddrRange stack_cluster_;
  RPCClient *rpc_client_;
};
} // namespace nu
