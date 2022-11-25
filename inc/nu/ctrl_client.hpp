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
#include "nu/utils/rpc.hpp"

namespace nu {

class ControllerClient;

class MigrationDest {
 public:
  ~MigrationDest();
  operator bool() const;
  NodeIP get_ip() const;

 private:
  ControllerClient *client_;
  NodeIP ip_;
  friend class ControllerClient;

  MigrationDest(ControllerClient *client, NodeIP ip);
};

class ControllerClient {
 public:
  ControllerClient(NodeIP ctrl_server_ip, Runtime::Mode mode, lpid_t lpid);
  std::optional<std::pair<lpid_t, VAddrRange>> register_node(NodeIP ip,
                                                             MD5Val md5);
  bool verify_md5(MD5Val md5);
  std::optional<std::pair<ProcletID, NodeIP>> allocate_proclet(
      uint64_t capacity, NodeIP ip_hint);
  void destroy_proclet(VAddrRange heap_segment);
  NodeIP resolve_proclet(ProcletID id);
  MigrationDest acquire_migration_dest(Resource resource);
  void update_location(ProcletID id, NodeIP proclet_srv_ip);
  VAddrRange get_stack_cluster() const;
  void report_free_resource(Resource resource);
  std::vector<std::pair<NodeIP, Resource>> get_free_resources();

 private:
  lpid_t lpid_;
  VAddrRange stack_cluster_;
  RPCClient *rpc_client_;
  std::unique_ptr<rt::TcpConn> tcp_conn_;
  rt::Spin spin_;
  friend class MigrationDest;

  void release_migration_dest(NodeIP ip);
};
}  // namespace nu
