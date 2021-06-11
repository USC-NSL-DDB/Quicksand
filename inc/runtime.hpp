#pragma once

#include <cstdint>
#include <memory>

extern "C" {
#include <runtime/net.h>
}

#include "defs.hpp"
#include "stack_allocator.hpp"
#include "utils/archive_pool.hpp"
#include "utils/future.hpp"
#include "utils/rcu_lock.hpp"
#include "utils/slab.hpp"

namespace nu {

class ObjServer;
class HeapManager;
class ControllerClient;
class RemObjConnManager;
class Migrator;
class Monitor;
struct HeapHeader;
template <typename T> class RuntimeAllocator;
template <typename T> class RuntimeDeleter;

template <typename T>
using RuntimeFuture = Future<T, RuntimeDeleter<Promise<T>>>;

class Runtime {
public:
  enum Mode { CLIENT, SERVER, CONTROLLER };

  constexpr static uint64_t kRuntimeHeapSize = 32ULL << 30;
  static SlabAllocator runtime_slab;

  ~Runtime();
  static std::unique_ptr<Runtime> init(uint16_t local_obj_srv_port,
                                       uint16_t local_migrator_port,
                                       netaddr ctrl_server_addr, Mode mode);
  static void reserve_ctrl_server_conns(uint32_t num);
  static void reserve_obj_server_conns(uint32_t num, netaddr obj_server_addr);
  static void reserve_migration_conns(uint32_t num, netaddr dest_server_addr);
  static HeapHeader *get_obj_heap_header();

private:
  static RCULock rcu_lock;
  static std::unique_ptr<ObjServer> obj_server;
  static std::unique_ptr<ControllerClient> controller_client;
  static std::unique_ptr<HeapManager> heap_manager;
  static std::unique_ptr<RemObjConnManager> rem_obj_conn_mgr;
  static std::unique_ptr<Migrator> migrator;
  static std::unique_ptr<Monitor> monitor;
  static std::unique_ptr<ArchivePool<RuntimeAllocator<uint8_t>>> archive_pool;

  friend class Test;
  friend class ObjServer;
  friend class ControllerClient;
  friend class RemObjConnManager;
  friend class Monitor;
  friend class Migrator;
  friend class Mutex;
  friend class CondVar;
  friend class Time;
  template <typename T> friend class RemObj;
  template <typename T> friend class RemPtr;
  template <typename T> friend class RuntimeDeleter;

  Runtime(uint16_t local_obj_srv_port, uint16_t local_migrator_port,
          netaddr ctrl_server_addr, Mode mode);
  static void init_runtime_heap();
  static void init_as_controller(netaddr ctrl_server_addr);
  static void init_as_server(uint16_t local_obj_srv_port,
                             uint16_t local_migrator_port,
                             netaddr ctrl_server_addr);
  static void init_as_client(netaddr ctrl_server_addr);
  template <typename Cls, typename Fn, typename... As>
  static bool run_within_obj_env(void *heap_base, Fn fn, As &&... args);
  template <typename Cls, typename Fn, typename... As>
  static void __run_within_obj_env(StackAllocator *stack_allocator,
                                   uint8_t *obj_stack, Cls *obj_ptr, Fn fn,
                                   As &&... args);
  static void switch_to_obj_heap(void *obj_ptr);
  static void switch_to_runtime_heap();
  static void migration_enable();
  static void migration_disable();

  template <typename T, typename... Args>
  static T *new_on_runtime_heap(Args &&... args);
  template <typename T> static void delete_on_runtime_heap(T *ptr);
};
} // namespace nu

#include "impl/runtime.ipp"
