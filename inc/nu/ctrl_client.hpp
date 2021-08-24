#pragma once

#include <functional>
#include <optional>

extern "C" {
#include <net/ip.h>
#include <runtime/net.h>
}
#include <net.h>
#include <sync.h>

#include "nu/ctrl_server.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"

namespace nu {

// TODO: return heap back.

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
  VAddrRange get_stack_cluster() const;

private:
  std::unique_ptr<RPCClient> rpc_client_;
  VAddrRange stack_cluster_;
};
} // namespace nu
