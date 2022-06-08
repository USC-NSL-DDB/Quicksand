#pragma once

#include <functional>
#include <utility>

extern "C" {
#include <net/ip.h>
#include <runtime/net.h>
}
#include <net.h>
#include <sync.h>

#include <memory>

#include "nu/commons.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/rpc_server.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/utils/rpc.hpp"

namespace nu {

class ControllerClient {
 public:
  ControllerClient(uint32_t ctrl_server_ip, Runtime::Mode mode, lpid_t lpid);
  std::optional<std::pair<lpid_t, VAddrRange>> register_node(const Node &node,
                                                             MD5Val md5);
  bool verify_md5(MD5Val md5);
  std::optional<std::pair<ProcletID, uint32_t>> allocate_proclet(
      uint32_t ip_hint);
  void destroy_proclet(ProcletID id);
  uint32_t resolve_proclet(ProcletID id);
  uint32_t get_migration_dest(Resource resource);
  void update_location(ProcletID id, uint32_t proclet_srv_ip);
  VAddrRange get_stack_cluster() const;
  void report_free_resource(Resource resource);

 private:
  lpid_t lpid_;
  VAddrRange stack_cluster_;
  RPCClient *rpc_client_;
  std::unique_ptr<rt::TcpConn> tcp_conn_;
  rt::Spin spin_;
};
}  // namespace nu
