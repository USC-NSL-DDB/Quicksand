#pragma once

#include <cstdint>

extern "C" {
#include <runtime/tcp.h>
}
#include <atomic>
#include <memory>
#include <net.h>
#include <thread.h>

#include "nu/commons.hpp"
#include "nu/ctrl.hpp"
#include "nu/rpc_server.hpp"
#include "nu/utils/rpc.hpp"

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

struct RPCReqReportFreeResource {
  RPCReqType rpc_type = kReportFreeResource;
  lpid_t lpid;
  uint32_t ip;
  Resource resource;
} __attribute__((packed));

class ControllerServer {
public:
  constexpr static bool kEnableLogging = false;
  constexpr static uint64_t kPrintIntervalUs = kOneSecond;
  constexpr static uint32_t kTCPListenBackLog = 64;
  constexpr static uint32_t kPort = 2828;

  ControllerServer();
  ~ControllerServer();

private:
  std::unique_ptr<rt::TcpQueue> tcp_queue_;
  Controller ctrl_;
  std::atomic<uint64_t> num_register_node_;
  std::atomic<uint64_t> num_verify_md5_;
  std::atomic<uint64_t> num_allocate_obj_;
  std::atomic<uint64_t> num_destroy_obj_;
  std::atomic<uint64_t> num_resolve_obj_;
  std::atomic<uint64_t> num_get_migration_dest_;
  std::atomic<uint64_t> num_update_location_;
  std::atomic<uint64_t> num_report_free_resource_;
  rt::Thread logging_thread_;
  rt::Thread tcp_queue_thread_;
  std::vector<std::unique_ptr<rt::TcpConn>> tcp_conns_;
  std::vector<rt::Thread> tcp_conn_threads_;
  bool done_;
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
  void handle_report_free_resource(const RPCReqReportFreeResource &req);
  void tcp_loop(rt::TcpConn *c);
};
} // namespace nu
