#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>

extern "C" {
#include <runtime/net.h>
}

#include "nu/commons.hpp"
#include "nu/rpc_server.hpp"
#include "nu/utils/archive_pool.hpp"
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
template <typename T>
class WeakProclet;
class MigrationGuard;

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
  static void reserve_conns(uint32_t ip);
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
  static void *switch_stack(void *new_rsp);
  static void switch_to_runtime_stack();
  static VAddrRange get_proclet_stack_range(thread_t *thread);
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
  template <typename T>
  static WeakProclet<T> get_current_weak_proclet();
  // Detach the current thread from the current proclet.
  static void detach(const MigrationGuard &g);
  // Attach the current thread to the specified proclet and disable migration.
  // Return std::nullopt if failed, or MigrationGuard if succeeded.
  // No migration will happen during its invocation.
  static std::optional<MigrationGuard> attach_and_disable_migration(
      ProcletHeader *proclet_header);
  // Reattach the current thread from an old proclet (with migration disabled)
  // into a new proclet. Return std::nullopt if failed, or MigrationGuard of the
  // new proclet if succeeded. No migration will happen during its invocation.
  static std::optional<MigrationGuard> reattach_and_disable_migration(
      ProcletHeader *new_header, const MigrationGuard &old_guard);

 private:
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
  friend class Thread;
  friend class RPCServer;
  friend class Controller;
  friend class ControllerClient;
  friend class PressureHandler;
  friend class ResourceReporter;
  template <typename T>
  friend class Proclet;

  Runtime(uint32_t remote_ctrl_ip, Mode mode, lpid_t lpid);
  template <typename Cls, typename... A0s, typename... A1s>
  static bool __run_within_proclet_env(void *proclet_base, void (*fn)(A0s...),
                                       A1s &&... args);
  static std::optional<MigrationGuard> __reattach_and_disable_migration(
      ProcletHeader *proclet_header);
};

class RuntimeSlabGuard {
 public:
  RuntimeSlabGuard();
  ~RuntimeSlabGuard();
  RuntimeSlabGuard(const RuntimeSlabGuard &) = delete;
  RuntimeSlabGuard &operator=(const RuntimeSlabGuard &) = delete;
  RuntimeSlabGuard(RuntimeSlabGuard &&) = delete;
  RuntimeSlabGuard &operator=(RuntimeSlabGuard &&) = delete;

 private:
  void *original_slab_;
};

class ProcletSlabGuard {
 public:
  ProcletSlabGuard(void *slab);
  ~ProcletSlabGuard();
  ProcletSlabGuard(const ProcletSlabGuard &) = delete;
  ProcletSlabGuard &operator=(const ProcletSlabGuard &) = delete;
  ProcletSlabGuard(ProcletSlabGuard &&) = delete;
  ProcletSlabGuard &operator=(ProcletSlabGuard &&) = delete;

 private:
  void *original_slab_;
};

// Once constructed, it guarantees that the current proclet won't be migrated.
// However, migration might happen during its construction.
class MigrationGuard {
 public:
  MigrationGuard();
  ~MigrationGuard();
  MigrationGuard(const MigrationGuard &) = delete;
  MigrationGuard &operator=(const MigrationGuard &) = delete;
  MigrationGuard(MigrationGuard &&);
  MigrationGuard &operator=(MigrationGuard &&) = delete;
  ProcletHeader *header() const;
  template <typename F>
  auto enable_for(F &&f);
  void reset();
  void release();

 private:
  ProcletHeader *header_;
  friend class Runtime;

  MigrationGuard(ProcletHeader *header);
};

int runtime_main_init(int argc, char **argv,
                      std::function<void(int argc, char **argv)> main_func);

}  // namespace nu

#include "nu/impl/runtime.ipp"
