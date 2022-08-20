#pragma once

#include <cstdint>

extern "C" {
#include <runtime/tcp.h>
}
#include <net.h>
#include <thread.h>

#include <atomic>
#include <memory>

#include "nu/commons.hpp"
#include "nu/ctrl.hpp"
#include "nu/rpc_server.hpp"
#include "nu/utils/rpc.hpp"

namespace nu {

struct RPCReqRegisterNode {
  RPCReqType rpc_type = kRegisterNode;
  NodeIP ip;
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

struct RPCReqAllocateProclet {
  RPCReqType rpc_type = kAllocateProclet;
  uint64_t capacity;
  lpid_t lpid;
  NodeIP ip_hint;
} __attribute__((packed));

struct RPCRespAllocateProclet {
  bool empty;
  ProcletID id;
  NodeIP server_ip;
} __attribute__((packed));

struct RPCReqDestroyProclet {
  RPCReqType rpc_type = kDestroyProclet;
  VAddrRange heap_segment;
} __attribute__((packed));

struct RPCRespDestroyProclet {
  bool ok;
} __attribute__((packed));

struct RPCReqResolveProclet {
  RPCReqType rpc_type = kResolveProclet;
  ProcletID id;
} __attribute__((packed));

struct RPCRespResolveProclet {
  NodeIP ip;
} __attribute__((packed));

struct RPCReqUpdateLocation {
  RPCReqType rpc_type = kUpdateLocation;
  ProcletID id;
  NodeIP proclet_srv_ip;
} __attribute__((packed));

struct RPCReqAcquireMigrationDest {
  RPCReqType rpc_type = kAcquireMigrationDest;
  lpid_t lpid;
  NodeIP src_ip;
  Resource resource;
} __attribute__((packed));

struct RPCRespAcquireMigrationDest {
  NodeIP ip;
} __attribute__((packed));

struct RPCReqReleaseMigrationDest {
  RPCReqType rpc_type = kReleaseMigrationDest;
  lpid_t lpid;
  NodeIP ip;
} __attribute__((packed));

struct RPCReqReportFreeResource {
  RPCReqType rpc_type = kReportFreeResource;
  lpid_t lpid;
  NodeIP ip;
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
  std::atomic<uint64_t> num_allocate_proclet_;
  std::atomic<uint64_t> num_destroy_proclet_;
  std::atomic<uint64_t> num_resolve_proclet_;
  std::atomic<uint64_t> num_acquire_migration_dest_;
  std::atomic<uint64_t> num_release_migration_dest_;
  std::atomic<uint64_t> num_update_location_;
  std::atomic<uint64_t> num_report_free_resource_;
  rt::Thread logging_thread_;
  rt::Thread tcp_queue_thread_;
  std::vector<std::unique_ptr<rt::TcpConn>> tcp_conns_;
  std::vector<rt::Thread> tcp_conn_threads_;
  bool done_;
  friend class RPCServer;

  std::unique_ptr<RPCRespRegisterNode> handle_register_node(
      const RPCReqRegisterNode &req);
  std::unique_ptr<RPCRespVerifyMD5> handle_verify_md5(
      const RPCReqVerifyMD5 &req);
  std::unique_ptr<RPCRespAllocateProclet> handle_allocate_proclet(
      const RPCReqAllocateProclet &req);
  std::unique_ptr<RPCRespDestroyProclet> handle_destroy_proclet(
      const RPCReqDestroyProclet &req);
  std::unique_ptr<RPCRespResolveProclet> handle_resolve_proclet(
      const RPCReqResolveProclet &req);
  std::unique_ptr<RPCRespAcquireMigrationDest> handle_acquire_migration_dest(
      const RPCReqAcquireMigrationDest &req);
  void handle_release_migration_dest(const RPCReqReleaseMigrationDest &req);
  void handle_update_location(const RPCReqUpdateLocation &req);
  void handle_report_free_resource(const RPCReqReportFreeResource &req);
  void tcp_loop(rt::TcpConn *c);
};
}  // namespace nu
