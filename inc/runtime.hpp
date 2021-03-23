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

class HeapManager;
class ControllerClient;
class RemObjConnManager;
class Migrator;
template <typename T> class RuntimeAllocator;
template <typename T> class RuntimeDeleter;

class Runtime {
public:
  enum Mode { CLIENT, SERVER, CONTROLLER };

  constexpr static uint64_t kRuntimeHeapSize = 32ULL << 30;
  static SlabAllocator runtime_slab;

  ~Runtime();
  static std::unique_ptr<Runtime> init(uint16_t local_obj_srv_port,
                                       netaddr remote_ctrl_addr, Mode mode);

private:
  template <typename T>
  using RuntimeFuture = Future<T, RuntimeDeleter<Promise<T>>>;

  static RCULock rcu_lock;
  static std::unique_ptr<ControllerClient> controller_client;
  static std::unique_ptr<HeapManager> heap_manager;
  static std::unique_ptr<RemObjConnManager> rem_obj_conn_mgr;
  static std::unique_ptr<Migrator> migrator;
  static std::unique_ptr<ThreadSafeHashMap<
      RemObjID, RuntimeFuture<void>,
      RuntimeAllocator<std::pair<const RemObjID, RuntimeFuture<void>>>>>
      obj_inflight_inc_cnts;

  friend class ObjServer;
  friend class ControllerClient;
  friend class RemObjConnManager;
  friend class Monitor;
  template <typename T> friend class RemObj;
  template <typename T> friend class RuntimeDeleter;

  Runtime(uint16_t local_obj_srv_port, netaddr remote_ctrl_addr, Mode mode);
  static void init_runtime_heap();
  static void init_as_controller(netaddr remote_ctrl_addr);
  static void init_as_server(uint16_t local_obj_srv_port,
                             netaddr remote_ctrl_addr);
  static void init_as_client(uint16_t local_obj_srv_port,
                             netaddr remote_ctrl_addr);
  template <typename T> static T *setup_thread_env(void *heap_base);
  static void clear_thread_env(void *heap_base);
  template <typename T> static T *switch_to_obj_heap(void *heap_base);
  static void switch_to_runtime_heap();
  template <typename T, typename... Args>
  static T *new_on_runtime_heap(Args &&... args);
  template <typename T> static void delete_on_runtime_heap(T *ptr);
};
} // namespace nu

#include "impl/runtime.ipp"
