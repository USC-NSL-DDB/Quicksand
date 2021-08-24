#pragma once

#include <cstdint>

extern "C" {
#include <runtime/tcp.h>
}
#include <memory>
#include <net.h>

#include "nu/ctrl.hpp"
#include "nu/utils/rpc.hpp"

namespace nu {

enum ControllerRPC_t {
  kRegisterNode,
  kAllocateObj,
  kDestroyObj,
  kResolveObj,
  kGetMigrationDest,
  kUpdateLocation,
};

struct RPCReqRegisterNode {
  ControllerRPC_t rpc_type = kRegisterNode;
  Node node;
} __attribute__((packed));

struct RPCRespRegisterNode {
  VAddrRange stack_cluster;
};

struct RPCReqAllocateObj {
  ControllerRPC_t rpc_type = kAllocateObj;
  netaddr hint;
} __attribute__((packed));

struct RPCRespAllocateObj {
  bool empty;
  RemObjID id;
  netaddr server_addr;
};

struct RPCReqDestroyObj {
  ControllerRPC_t rpc_type = kDestroyObj;
  RemObjID id;
} __attribute__((packed));

struct RPCRespDestroyObj {
  bool ok;
};

struct RPCReqResolveObj {
  ControllerRPC_t rpc_type = kResolveObj;
  RemObjID id;
} __attribute__((packed));

struct RPCRespResolveObj {
  bool empty;
  netaddr addr;
};

struct RPCReqUpdateLocation {
  ControllerRPC_t rpc_type = kUpdateLocation;
  RemObjID id;
  netaddr obj_srv_addr;
} __attribute__((packed));

struct RPCReqGetMigrationDest {
  ControllerRPC_t rpc_type = kGetMigrationDest;
  uint32_t src_ip;
  Resource resource;
} __attribute__((packed));

struct RPCRespGetMigrationDest {
  bool empty;
  netaddr addr;
};

class ControllerServer {
public:
  constexpr static uint32_t kTCPListenBackLog = 64;
  constexpr static uint32_t kControllerServerPort = 8000;

  ControllerServer();
  void run_loop();

private:
  std::unique_ptr<rt::TcpQueue> tcp_queue_;
  Controller ctrl_;
  void handle_req(std::span<std::byte> args, RPCReturner *return_buf);
  std::unique_ptr<RPCRespRegisterNode>
  handle_register_node(const RPCReqRegisterNode &req);
  std::unique_ptr<RPCRespAllocateObj>
  handle_allocate_obj(const RPCReqAllocateObj &req);
  std::unique_ptr<RPCRespDestroyObj>
  handle_destroy_obj(const RPCReqDestroyObj &req);
  std::unique_ptr<RPCRespResolveObj>
  handle_resolve_obj(const RPCReqResolveObj &req);
  std::unique_ptr<RPCRespGetMigrationDest>
  handle_get_migration_dest(const RPCReqGetMigrationDest &req);
  void handle_update_location(const RPCReqUpdateLocation &req);
};
} // namespace nu
