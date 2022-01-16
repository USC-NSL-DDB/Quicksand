#pragma once

#include <cstdint>

extern "C" {
#include <runtime/tcp.h>
}
#include <memory>
#include <net.h>

#include "nu/commons.hpp"
#include "nu/ctrl.hpp"
#include "nu/utils/rpc.hpp"
#include "nu/rpc_server.hpp"

namespace nu {

struct RPCReqRegisterNode {
  RPCReqType rpc_type = kRegisterNode;
  Node node;
  lpid_t lpid;
  MD5Val md5;
} __attribute__((packed));

struct RPCRespRegisterNode {
  bool empty;
  lpid_t lpid;
  VAddrRange stack_cluster;
} __attribute__((packed));

struct RPCReqVerifyMD5 {
  RPCReqType rpc_type = kVerifyMD5;
  lpid_t lpid;
  MD5Val md5;
} __attribute__((packed));

struct RPCRespVerifyMD5 {
  bool passed;
} __attribute__((packed));

struct RPCReqAllocateObj {
  RPCReqType rpc_type = kAllocateObj;
  lpid_t lpid;
  uint32_t ip_hint;
} __attribute__((packed));

struct RPCRespAllocateObj {
  bool empty;
  RemObjID id;
  uint32_t server_ip;
} __attribute__((packed));

struct RPCReqDestroyObj {
  RPCReqType rpc_type = kDestroyObj;
  RemObjID id;
} __attribute__((packed));

struct RPCRespDestroyObj {
  bool ok;
} __attribute__((packed));

struct RPCReqResolveObj {
  RPCReqType rpc_type = kResolveObj;
  RemObjID id;
} __attribute__((packed));

struct RPCRespResolveObj {
  uint32_t ip;
} __attribute__((packed));

struct RPCReqUpdateLocation {
  RPCReqType rpc_type = kUpdateLocation;
  RemObjID id;
  uint32_t obj_srv_ip;
} __attribute__((packed));

struct RPCReqGetMigrationDest {
  RPCReqType rpc_type = kGetMigrationDest;
  lpid_t lpid;
  uint32_t src_ip;
  Resource resource;
} __attribute__((packed));

struct RPCRespGetMigrationDest {
  uint32_t ip;
} __attribute__((packed));

struct RPCReqProbeFreeResource {
  RPCReqType rpc_type = kProbeFreeResource;
} __attribute__((packed));

struct RPCRespProbeFreeResource {
  Resource resource;
} __attribute__((packed));

class ControllerServer {
public:
  constexpr static uint32_t kTCPListenBackLog = 64;
  constexpr static uint32_t kPort = 8000;

  ControllerServer();

private:
  std::unique_ptr<rt::TcpQueue> tcp_queue_;
  Controller ctrl_;
  friend class RPCServer;

  std::unique_ptr<RPCRespRegisterNode>
  handle_register_node(const RPCReqRegisterNode &req);
  std::unique_ptr<RPCRespVerifyMD5>
  handle_verify_md5(const RPCReqVerifyMD5 &req);
  std::unique_ptr<RPCRespAllocateObj>
  handle_allocate_obj(const RPCReqAllocateObj &req);
  std::unique_ptr<RPCRespDestroyObj>
  handle_destroy_obj(const RPCReqDestroyObj &req);
  std::unique_ptr<RPCRespResolveObj>
  handle_resolve_obj(const RPCReqResolveObj &req);
  std::unique_ptr<RPCRespGetMigrationDest>
  handle_get_migration_dest(const RPCReqGetMigrationDest &req);
  void handle_update_location(const RPCReqUpdateLocation &req);
  std::unique_ptr<RPCRespProbeFreeResource>
  handle_probing(const RPCReqProbeFreeResource &req);
};
} // namespace nu
