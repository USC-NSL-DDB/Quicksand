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
#include "nu/utils/archive_pool.hpp"
#include "nu/utils/future.hpp"
#include "nu/utils/rcu_lock.hpp"
#include "nu/utils/rpc.hpp"
#include "nu/utils/slab.hpp"

namespace nu {

struct HeapHeader;
class ObjServer;
class HeapManager;
class StackManager;
class ControllerClient;
class ControllerServer;
class RPCClientMgr;
class Migrator;
class RPCServer;
template <typename T> class RuntimeAllocator;
template <typename T> class RuntimeDeleter;

template <typename T>
using RuntimeFuture = Future<T, RuntimeDeleter<Promise<T>>>;

class Runtime {
public:
  enum Mode { CLIENT, SERVER, CONTROLLER };

  static SlabAllocator runtime_slab;

  ~Runtime();
  static std::unique_ptr<Runtime> init(uint32_t remote_ctrl_ip, Mode mode);
  static uint32_t get_ip_by_rem_obj_id(RemObjID id);
  static void reserve_conn(uint32_t ip);

private:
  static RCULock rcu_lock;
  static std::unique_ptr<ObjServer> obj_server;
  static std::unique_ptr<ControllerClient> controller_client;
  static std::unique_ptr<ControllerServer> controller_server;
  static std::unique_ptr<HeapManager> heap_manager;
  static std::unique_ptr<StackManager> stack_manager;
  static std::unique_ptr<RPCClientMgr> rpc_client_mgr;
  static std::unique_ptr<Migrator> migrator;
  static std::unique_ptr<ArchivePool<RuntimeAllocator<uint8_t>>> archive_pool;
  static std::unique_ptr<RPCServer> rpc_server;

  friend class Test;
  friend class ObjServer;
  friend class RPCClientMgr;
  friend class Migrator;
  friend class Mutex;
  friend class CondVar;
  friend class Time;
  friend class CPULoad;
  friend class HeapManager;
  friend class RPCServer;
  friend class Controller;
  friend class ControllerClient;
  friend class PressureHandler;
  friend class DistributedMemPool;
  friend class RuntimeHeapGuard;
  friend class ObjHeapGuard;
  friend class MigrationEnabledGuard;
  friend class MigrationDisabledGuard;
  friend class OutermostMigrationDisabledGuard;
  template <typename T> friend class RemObj;
  template <typename T> friend class RemRawPtr;
  template <typename T> friend class RemUniquePtr;
  template <typename T> friend class RemSharedPtr;

  Runtime(uint32_t remote_ctrl_ip, Mode mode);
  static void common_init();
  static void init_runtime_heap();
  static void init_as_controller();
  static void init_as_server(uint32_t remote_ctrl_ip);
  static void init_as_client(uint32_t remote_ctrl_ip);
  template <typename Cls, typename... A0s, typename... A1s>
  static bool run_within_obj_env(void *heap_base, void (*fn)(A0s...),
                                 A1s &&... args);
  static void switch_to_obj_heap(void *obj_ptr);
  static void switch_to_runtime_heap();
  template <typename T, typename... Args>
  static T *new_on_runtime_heap(Args &&... args);
  template <typename T> static void delete_on_runtime_heap(T *ptr);
  static HeapHeader *get_current_obj_heap_header();
  static RemObjID get_current_obj_id();
  template <typename T> static T *get_current_obj();
  template <typename T> static T *get_obj(RemObjID id);
};

class RuntimeHeapGuard {
public:
  RuntimeHeapGuard();
  ~RuntimeHeapGuard();

private:
  void *original_heap_;
};

class ObjHeapGuard {
public:
  ObjHeapGuard(void *obj_ptr);
  ~ObjHeapGuard();

private:
  void *original_heap_;
};

int runtime_main_init(int argc, char **argv,
                      std::function<void(int argc, char **argv)> main_func);

} // namespace nu

#include "nu/impl/runtime.ipp"
