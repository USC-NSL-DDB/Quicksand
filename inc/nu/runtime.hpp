#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

extern "C" {
#include <runtime/net.h>
}

#include "nu/commons.hpp"
#include "nu/rpc_server.hpp"
#include "nu/utils/archive_pool.hpp"
#include "nu/utils/rcu_lock.hpp"
#include "nu/utils/rpc.hpp"
#include "nu/utils/slab.hpp"

namespace nu {

struct ProcletHeader;
class ProcletServer;
class ProcletManager;
class StackManager;
class ControllerClient;
class ControllerServer;
class RPCClientMgr;
class Migrator;
class RPCServer;
class PressureHandler;
class ResourceReporter;
template <typename T>
class RuntimeAllocator;
template <typename T>
class RuntimeDeleter;

struct RPCReqReserveConns {
  RPCReqType rpc_type = kReserveConns;
  uint32_t dest_server_ip;
} __attribute__((packed));

class Runtime {
 public:
  enum Mode { kClient, kServer, kController };

  static SlabAllocator runtime_slab;

  ~Runtime();
  static std::unique_ptr<Runtime> init(uint32_t remote_ctrl_ip, Mode mode,
                                       lpid_t lpid);
  static uint32_t get_ip_by_proclet_id(ProcletID id);
  static void reserve_conns(uint32_t ip);

 private:
  static RCULock rcu_lock;
  static std::unique_ptr<ProcletServer> proclet_server;
  static std::unique_ptr<ControllerClient> controller_client;
  static std::unique_ptr<ControllerServer> controller_server;
  static std::unique_ptr<ProcletManager> proclet_manager;
  static std::unique_ptr<StackManager> stack_manager;
  static std::unique_ptr<RPCClientMgr> rpc_client_mgr;
  static std::unique_ptr<Migrator> migrator;
  static std::unique_ptr<ArchivePool<RuntimeAllocator<uint8_t>>> archive_pool;
  static std::unique_ptr<RPCServer> rpc_server;
  static std::unique_ptr<PressureHandler> pressure_handler;
  static std::unique_ptr<ResourceReporter> resource_reporter;

  friend class Test;
  friend class ProcletServer;
  friend class RPCClientMgr;
  friend class Migrator;
  friend class Mutex;
  friend class CondVar;
  friend class Time;
  friend class Thread;
  friend class CPULoad;
  friend class ProcletManager;
  friend class RPCServer;
  friend class Controller;
  friend class ControllerClient;
  friend class PressureHandler;
  friend class ResourceReporter;
  friend class DistributedMemPool;
  friend class RuntimeSlabGuard;
  friend class ProcletSlabGuard;
  friend class MigrationEnabledGuard;
  friend class MigrationDisabledGuard;
  friend class NonBlockingMigrationDisabledGuard;
  template <typename T>
  friend class Proclet;
  template <typename T>
  friend class RemRawPtr;
  template <typename T>
  friend class RemUniquePtr;
  template <typename T>
  friend class RemSharedPtr;

  Runtime(uint32_t remote_ctrl_ip, Mode mode, lpid_t lpid);
  static void common_init();
  static void init_runtime_heap();
  static void init_as_controller();
  static void init_as_server(uint32_t remote_ctrl_ip, lpid_t lpid);
  static void init_as_client(uint32_t remote_ctrl_ip, lpid_t lpid);
  template <typename Cls, typename... A0s, typename... A1s>
  static bool run_within_proclet_env(void *proclet_base, void (*fn)(A0s...),
                                     A1s &&... args);
  static void *switch_slab(void *slab);
  static void *switch_to_runtime_slab();
  template <typename T, typename... Args>
  static T *new_on_runtime_heap(Args &&... args);
  template <typename T>
  static void delete_on_runtime_heap(T *ptr);
  static SlabAllocator *get_current_proclet_slab();
  static ProcletHeader *get_current_proclet_header();
  template <typename T>
  static T *get_current_root_obj();
  template <typename T>
  static T *get_root_obj(ProcletID id);
};

class RuntimeSlabGuard {
 public:
  RuntimeSlabGuard();
  ~RuntimeSlabGuard();

 private:
  void *original_slab_;
};

class ProcletSlabGuard {
 public:
  ProcletSlabGuard(void *slab);
  ~ProcletSlabGuard();

 private:
  void *original_slab_;
};

int runtime_main_init(int argc, char **argv,
                      std::function<void(int argc, char **argv)> main_func);

}  // namespace nu

#include "nu/impl/runtime.ipp"
