#pragma once

#include <cstdint>

extern "C" {
#include <runtime/tcp.h>
}

#include "ctrl.hpp"
#include "utils/tcp.hpp"

namespace nu {

enum ControllerRPC_t {
  REGISTER_NODE,
  ALLOCATE_OBJ,
  DESTROY_OBJ,
  RESOLVE_OBJ,
  GET_MIGRATION_DEST,
  UPDATE_LOCATION,
};

struct RPCReqRegisterNode {
  Node node;
} __attribute__((packed));

struct RPCRespRegisterNode {
} __attribute__((packed));

struct RPCReqAllocateObj {
} __attribute__((packed));

struct RPCRespAllocateObj {
  bool empty;
  RemObjID id;
  VAddrRange range;
} __attribute__((packed));

struct RPCReqDestroyObj {
  RemObjID id;
} __attribute__((packed));

struct RPCRespDestroyObj {
  bool ok;
} __attribute__((packed));

struct RPCReqResolveObj {
  RemObjID id;
} __attribute__((packed));

struct RPCRespResolveObj {
  bool empty;
  netaddr addr;
} __attribute__((packed));

struct RPCReqUpdateLocation {
  RemObjID id;
  netaddr obj_srv_addr;
} __attribute__((packed));

struct RPCRespUpdateLocation {
} __attribute__((packed));

struct RPCReqGetMigrationDest {
  Resource resource;
} __attribute__((packed));

struct RPCRespGetMigrationDest {
  bool empty;
  netaddr addr;
} __attribute__((packed));

class ControllerServer {
public:
  constexpr static uint32_t kTCPListenBackLog = 64;

  ControllerServer(uint16_t port);
  void run_loop();

private:
  tcpqueue_t *tcp_queue_;
  Controller ctrl_;

  void handle_reqs(tcpconn_t *c);
  bool handle_one_req(ControllerRPC_t rpc_type, tcpconn_t *c);
  bool handle_register_node(tcpconn_t *c);
  bool handle_allocate_obj(tcpconn_t *c);
  bool handle_destroy_obj(tcpconn_t *c);
  bool handle_resolve_obj(tcpconn_t *c);
  bool handle_get_migration_dest(tcpconn_t *c);
  bool handle_update_location(tcpconn_t *c);
};
} // namespace nu
