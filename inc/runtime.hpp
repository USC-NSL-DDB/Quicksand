#pragma once

#include <cstdint>
#include <memory>

extern "C" {
#include <runtime/net.h>
}

#include "defs.hpp"
#include "utils/future.hpp"
#include "utils/rcu_lock.hpp"
#include "utils/slab.hpp"
#include "utils/ts_hash_map.hpp"

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
                                       uint16_t local_migra_ldr_port,
                                       netaddr remote_ctrl_addr, Mode mode);

private:
  static RCULock rcu_lock;
  static std::unique_ptr<ObjServer> obj_server;
  static std::unique_ptr<ControllerClient> controller_client;
  static std::unique_ptr<HeapManager> heap_manager;
  static std::unique_ptr<RemObjConnManager> rem_obj_conn_mgr;
  static std::unique_ptr<Migrator> migrator;
  static std::unique_ptr<Monitor> monitor;
  static std::unique_ptr<ThreadSafeHashMap<
      RemObjID, RuntimeFuture<void>,
      RuntimeAllocator<std::pair<const RemObjID, RuntimeFuture<void>>>>>
      obj_inflight_inc_cnts;

  friend class Test;
  friend class ObjServer;
  friend class ControllerClient;
  friend class RemObjConnManager;
  friend class Monitor;
  friend class Migrator;
  template <typename T> friend class RemObj;
  template <typename T> friend class RuntimeDeleter;

  Runtime(uint16_t local_obj_srv_port, uint16_t local_migra_ldr_port,
          netaddr remote_ctrl_addr, Mode mode);
  static void init_runtime_heap();
  static void init_as_controller(netaddr remote_ctrl_addr);
  static void init_as_server(uint16_t local_obj_srv_port,
                             uint16_t local_migra_ldr_port,
                             netaddr remote_ctrl_addr);
  static void init_as_client(netaddr remote_ctrl_addr);
  template <typename Cls, typename Fn, typename... As>
  static bool run_within_obj_env(void *heap_base, Fn fn, As &&... args);
  template <typename Cls, typename Fn, typename... As>
  static void __run_within_obj_env(SlabAllocator *slab, uint64_t obj_stack_base,
                                   Cls *obj_ptr, Fn fn, As &&... args);
  static void migration_enable();
  static void migration_disable();
  
  template <typename T, typename... Args>
  static T *new_on_runtime_heap(Args &&... args);
  template <typename T> static void delete_on_runtime_heap(T *ptr);
};
} // namespace nu

#include "impl/runtime.ipp"
